package webhooks

import com.sun.net.httpserver.HttpServer
import java.io.InputStream
import java.net.InetSocketAddress
import java.nio.file.Path
import java.security.MessageDigest
import java.util.HexFormat
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

data class Delivery(val id: String, val body: ByteArray)

interface WebhookHandler {
    val vendor: String
    val persistWebhooks: Boolean get() = false
    val processingMode: ProcessingMode get() = ProcessingMode.SYNC
    fun handle(delivery: Delivery)
}

internal val vendorName = Regex("[a-z][a-z0-9-]{0,62}")
private val deliveryId = Regex("[A-Za-z0-9._:-]{1,128}")
private val signatureFormat = Regex("sha256=[A-Fa-f0-9]{64}")
private const val MAX_BODY_BYTES = 1024 * 1024

internal class RunningWebhookServer(
    private val server: HttpServer,
    private val executor: ExecutorService,
    private val worker: AutoCloseable?
) : AutoCloseable {
    val port: Int get() = server.address.port

    override fun close() {
        server.stop(1)
        executor.shutdownNow()
        worker?.close()
    }
}

internal fun startWebhookServer(
    address: InetSocketAddress,
    handler: WebhookHandler,
    secret: ByteArray,
    dataDirectory: Path,
    archive: WebhookArchive? = null,
    jobs: WebhookJobs? = null
): RunningWebhookServer {
    val endpoint = WebhookEndpoint(handler, secret, dataDirectory, archive, jobs)
    val server = HttpServer.create(address, 0)
    val executor = Executors.newFixedThreadPool(8)
    server.executor = executor
    server.createContext(endpoint.route) { exchange ->
        exchange.use {
            val status = try {
                endpoint.receive(
                    exchange.requestMethod,
                    exchange.requestURI.rawPath,
                    exchange.requestHeaders["X-Webhook-Delivery"]?.singleOrNull(),
                    exchange.requestHeaders["X-Webhook-Signature"]?.singleOrNull(),
                    exchange.requestBody
                )
            } catch (_: java.io.IOException) {
                500
            }
            if (status == 405) exchange.responseHeaders.set("Allow", "POST")
            if (status == 503) exchange.responseHeaders.set("Retry-After", "1")
            exchange.sendResponseHeaders(status, -1)
        }
    }
    server.start()
    val worker = if (jobs != null && handler.processingMode != ProcessingMode.SYNC) {
        WebhookWorker(handler, jobs).start()
    } else null
    return RunningWebhookServer(server, executor, worker)
}

internal class WebhookEndpoint(
    private val handler: WebhookHandler,
    secret: ByteArray,
    dataDirectory: Path,
    archive: WebhookArchive? = null,
    jobs: WebhookJobs? = null
) {
    private val mode = handler.processingMode
    private val queue = if (mode != ProcessingMode.SYNC) requireNotNull(jobs) else null
    private val archive = if (mode == ProcessingMode.SYNC && handler.persistWebhooks) requireNotNull(archive) else null

    init {
        require(vendorName.matches(handler.vendor))
        require(mode != ProcessingMode.ASYNC_ARCHIVE || handler.persistWebhooks)
        require(mode != ProcessingMode.ASYNC_PROJECTED || handler is ProjectedWebhookHandler)
    }

    val route = "/webhooks/${handler.vendor}"
    private val key = SecretKeySpec(secret, "HmacSHA256")
    private val store = if (mode == ProcessingMode.SYNC) DeliveryStore(dataDirectory.resolve(handler.vendor)) else null

    fun receive(method: String, path: String, id: String?, signature: String?, body: InputStream): Int {
        if (path != route) return 404
        if (method != "POST") return 405
        if (id == null || !deliveryId.matches(id)) return 400
        if (signature == null || !signatureFormat.matches(signature)) return 401
        val payload = body.readNBytes(MAX_BODY_BYTES + 1)
        if (payload.size > MAX_BODY_BYTES) return 413

        val mac = Mac.getInstance("HmacSHA256")
        mac.init(key)
        mac.update(id.toByteArray(Charsets.UTF_8))
        mac.update(10.toByte())
        val supplied = HexFormat.of().parseHex(signature.substringAfter('='))
        if (!MessageDigest.isEqual(mac.doFinal(payload), supplied)) return 401

        val delivery = Delivery(id, payload)
        queue?.let { return enqueue(it, delivery) }
        return try {
            when (checkNotNull(store).process(id) {
                try {
                    archive?.save(handler.vendor, delivery)
                } catch (failure: Exception) {
                    throw WebhookArchiveUnavailable(failure)
                }
                handler.handle(delivery)
            }) {
                DeliveryStore.Result.PROCESSED, DeliveryStore.Result.DUPLICATE -> 204
                DeliveryStore.Result.BUSY -> 503
            }
        } catch (failure: WebhookArchiveUnavailable) {
            System.err.println("Webhook archive unavailable for ${handler.vendor} (${failure.cause?.javaClass?.simpleName})")
            503
        } catch (failure: Exception) {
            System.err.println("Webhook processing failed for ${handler.vendor} (${failure.javaClass.simpleName})")
            500
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
            queue.enqueue(handler.vendor, delivery, projection, handler.persistWebhooks)
            204
        } catch (failure: Exception) {
            System.err.println("Webhook enqueue failed for ${handler.vendor} (${failure.javaClass.simpleName})")
            503
        }
    }

}

private class WebhookArchiveUnavailable(cause: Exception) : RuntimeException(cause)
