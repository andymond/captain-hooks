package webhooks

import java.util.UUID
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.coroutines.coroutineContext

enum class ProcessingMode { SYNC, ASYNC_ARCHIVE, ASYNC_PROJECTED }

interface ProjectedWebhookHandler : WebhookHandler {
    override val processingMode: ProcessingMode get() = ProcessingMode.ASYNC_PROJECTED

    // Reject invalid input with IllegalArgumentException. handle receives the resulting bytes.
    fun project(delivery: Delivery): ByteArray
}

class PermanentWebhookFailure(message: String) : RuntimeException(message)

data class ClaimedWebhook(
    val vendor: String, val delivery: Delivery, val attempt: Int, val token: UUID,
    val externalArchive: Boolean = false
)

interface WebhookJobs {
    fun enqueue(vendor: String, delivery: Delivery, projected: ByteArray?, archive: Boolean)
    /** Enqueue an archive reference. Implementations may override this to support external archives. */
    fun enqueueArchiveReference(vendor: String, deliveryId: String) {
        throw UnsupportedOperationException("This queue does not support external archive references")
    }

    val supportsExternalArchive: Boolean get() = false
    val supportsEmbeddedArchive: Boolean get() = true

    fun contains(vendor: String, id: String): Boolean
    fun claim(vendor: String, projected: Boolean): ClaimedWebhook?
    fun complete(job: ClaimedWebhook)
    fun fail(job: ClaimedWebhook, terminal: Boolean, delaySeconds: Int, failure: String)
}

internal const val MAX_WEBHOOK_ATTEMPTS = 5

class WebhookWorker(
    private val handler: WebhookHandler,
    private val jobs: WebhookJobs,
    private val archive: ReadableWebhookArchive? = null
) {
    suspend fun runOnce(): Boolean {
        val job = runInterruptible(Dispatchers.IO) {
            jobs.claim(handler.vendor, handler.processingMode == ProcessingMode.ASYNC_PROJECTED)
        } ?: return false

        try {
            runInterruptible(Dispatchers.IO) {
                val delivery = if (job.externalArchive) {
                    val body = checkNotNull(archive?.load(job.vendor, job.delivery.id)) {
                        "Archived delivery is unavailable"
                    }
                    Delivery(job.delivery.id, body)
                } else job.delivery
                handler.handle(delivery)
            }
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Exception) {
            coroutineContext.ensureActive()
            val terminal = failure is PermanentWebhookFailure || job.attempt >= MAX_WEBHOOK_ATTEMPTS
            runInterruptible(Dispatchers.IO) {
                jobs.fail(job, terminal, 1 shl (job.attempt - 1), failure.javaClass.simpleName)
            }
            return true
        }

        runInterruptible(Dispatchers.IO) { jobs.complete(job) }
        return true
    }

    fun start(): AutoCloseable {
        val poller = CoroutineScope(Dispatchers.IO).launch {
            while (isActive) {
                val worked = try {
                    runOnce()
                } catch (failure: CancellationException) {
                    throw failure
                } catch (failure: Exception) {
                    ensureActive()
                    System.err.println("Webhook worker failed for ${handler.vendor} (${failure.javaClass.simpleName})")
                    false
                }
                if (!worked) delay(250)
            }
        }
        return AutoCloseable {
            poller.cancel()
            runBlocking { withTimeoutOrNull(1_000) { poller.join() } }
        }
    }
}
