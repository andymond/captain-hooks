package webhooks

import java.io.InputStream
import java.nio.file.Path
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runInterruptible
import webhooks.signatures.CaptainHooksSignatureVerifier

data class Delivery(val id: String, val body: ByteArray)

interface WebhookHandler {
    val vendor: String
    val persistWebhooks: Boolean get() = false
    val processingMode: ProcessingMode get() = ProcessingMode.SYNC
    fun handle(delivery: Delivery)
}

internal val vendorName = Regex("[a-z][a-z0-9_]{0,62}")
internal val deliveryIdFormat = Regex("[A-Za-z0-9._:-]{1,128}")
private const val MAX_BODY_BYTES = 1024 * 1024

class WebhookEndpoint @JvmOverloads constructor(
    private val handler: WebhookHandler,
    private val verifier: WebhookSignatureVerifier,
    receipts: DeliveryReceipts? = null,
    archive: WebhookArchive? = null,
    jobs: WebhookJobs? = null
) {
    /** Uses the built-in signature format accepted by the CLI demos. */
    @JvmOverloads
    constructor(
        handler: WebhookHandler,
        secret: ByteArray,
        receipts: DeliveryReceipts? = null,
        archive: WebhookArchive? = null,
        jobs: WebhookJobs? = null
    ) : this(handler, CaptainHooksSignatureVerifier(secret), receipts, archive, jobs)

    /** Convenience constructor for applications using local filesystem receipts. */
    @JvmOverloads
    constructor(
        handler: WebhookHandler,
        secret: ByteArray,
        dataDirectory: Path,
        archive: WebhookArchive? = null,
        jobs: WebhookJobs? = null
    ) : this(handler, secret, FileDeliveryReceipts(dataDirectory), archive, jobs)

    private val mode = handler.processingMode
    private val receipts = if (mode == ProcessingMode.SYNC) {
        requireNotNull(receipts) { "Synchronous handling requires delivery receipts." }
    } else null
    private val queue = if (mode != ProcessingMode.SYNC) requireNotNull(jobs) else null
    private val archive = archive.takeIf { handler.persistWebhooks }
    private val readableArchive = this.archive as? ReadableWebhookArchive

    init {
        require(vendorName.matches(handler.vendor))
        require(mode != ProcessingMode.ASYNC_ARCHIVE || handler.persistWebhooks)
        require(mode != ProcessingMode.ASYNC_PROJECTED || handler is ProjectedWebhookHandler)
        require(!handler.persistWebhooks || this.archive != null) {
            "persistWebhooks requires an archive."
        }
        require(mode != ProcessingMode.ASYNC_ARCHIVE || readableArchive != null) {
            "Async archive mode requires a readable archive."
        }
    }

    val route = "/webhooks/${handler.vendor}"

    /** Start the worker when the host starts and close it when the host stops. */
    fun startWorker(): AutoCloseable? = queue?.let { WebhookWorker(handler, it, readableArchive).start() }

    /** Route-facing entry point. Supply the raw body and all header values. */
    fun handle(method: String, headers: Map<String, List<String>>, body: InputStream): WebhookResponse {
        val status = try {
            receive(method, route, headers, body)
        } catch (_: java.io.IOException) {
            500
        }
        return WebhookResponse(status)
    }

    fun handle(method: String, deliveryIds: List<String>, signatures: List<String>, body: InputStream): WebhookResponse =
        handle(method, demoHeaders(deliveryIds, signatures), body)

    suspend fun handleAsync(
        method: String, headers: Map<String, List<String>>, body: InputStream
    ): WebhookResponse = runInterruptible(Dispatchers.IO) { handle(method, headers, body) }

    suspend fun handleAsync(
        method: String, deliveryIds: List<String>, signatures: List<String>, body: InputStream
    ): WebhookResponse = handleAsync(method, demoHeaders(deliveryIds, signatures), body)

    fun receive(method: String, path: String, id: String?, signature: String?, body: InputStream): Int =
        receive(method, path, demoHeaders(listOfNotNull(id), listOfNotNull(signature)), body)

    fun receive(method: String, path: String, headers: Map<String, List<String>>, body: InputStream): Int {
        if (path != route) return 404
        if (method != "POST") return 405
        val payload = body.readNBytes(MAX_BODY_BYTES + 1)
        if (payload.size > MAX_BODY_BYTES) return 413

        val id = when (val result = verifier.verify(headers, payload.copyOf())) {
            SignatureVerification.InvalidSignature -> return 401
            SignatureVerification.InvalidDelivery -> return 400
            is SignatureVerification.Verified -> result.deliveryId
        }
        if (!deliveryIdFormat.matches(id)) return 400
        val delivery = Delivery(id, payload)
        queue?.let { return enqueue(it, delivery) }
        return try {
            when (checkNotNull(receipts).process(handler.vendor, id) {
                try {
                    archive?.save(handler.vendor, delivery)
                } catch (failure: Exception) {
                    throw WebhookArchiveUnavailable(failure)
                }
                try {
                    handler.handle(delivery)
                } catch (failure: Exception) {
                    throw WebhookHandlerFailure(failure)
                }
            }) {
                ReceiptResult.PROCESSED, ReceiptResult.DUPLICATE -> 204
                ReceiptResult.BUSY -> 503
            }
        } catch (failure: WebhookArchiveUnavailable) {
            System.err.println("Webhook archive unavailable for ${handler.vendor} (${failure.cause?.javaClass?.simpleName})")
            503
        } catch (failure: WebhookHandlerFailure) {
            System.err.println("Webhook processing failed for ${handler.vendor} (${failure.cause?.javaClass?.simpleName})")
            500
        } catch (failure: Exception) {
            System.err.println("Webhook receipts unavailable for ${handler.vendor} (${failure.javaClass.simpleName})")
            503
        }
    }
    private fun enqueue(queue: WebhookJobs, delivery: Delivery): Int {
        val projection = if (mode == ProcessingMode.ASYNC_PROJECTED) {
            try {
                (handler as ProjectedWebhookHandler).project(Delivery(delivery.id, delivery.body.copyOf())).also {
                    require(it.size <= MAX_BODY_BYTES)
                }
            } catch (_: IllegalArgumentException) {
                return try {
                    if (queue.contains(handler.vendor, delivery.id)) 204 else 400
                } catch (_: Exception) {
                    503
                }
            } catch (failure: Exception) {
                System.err.println("Webhook projection failed for ${handler.vendor} (${failure.javaClass.simpleName})")
                return 500
            }
        } else null
        return try {
            if (archive == null || !queue.contains(handler.vendor, delivery.id)) {
                archive?.save(handler.vendor, delivery)
                val input = if (projection == null) JobInput.Archived else JobInput.Projected(projection)
                queue.enqueue(WebhookJob(handler.vendor, delivery.id, input))
            }
            204
        } catch (failure: Exception) {
            System.err.println("Webhook enqueue failed for ${handler.vendor} (${failure.javaClass.simpleName})")
            503
        }
    }

}

private fun demoHeaders(deliveryIds: List<String>, signatures: List<String>): Map<String, List<String>> =
    mapOf("X-Webhook-Delivery" to deliveryIds, "X-Webhook-Signature" to signatures)

private class WebhookHandlerFailure(cause: Exception) : RuntimeException(cause)

private class WebhookArchiveUnavailable(cause: Exception) : RuntimeException(cause)

data class WebhookResponse(val status: Int) {
    val headers: Map<String, String>
        get() = when (status) {
            405 -> mapOf("Allow" to "POST")
            503 -> mapOf("Retry-After" to "1")
            else -> emptyMap()
        }
}
