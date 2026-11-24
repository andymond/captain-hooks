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

enum class JobKind { PROJECTED, ARCHIVE }

sealed interface JobInput {
    val kind: JobKind

    data class Projected(val body: ByteArray) : JobInput {
        override val kind = JobKind.PROJECTED
    }

    /** The worker loads the body by vendor and delivery ID from its configured archive. */
    data object Archived : JobInput {
        override val kind = JobKind.ARCHIVE
    }
}

data class WebhookJob(val vendor: String, val deliveryId: String, val input: JobInput)

data class ClaimedWebhook(val job: WebhookJob, val attempt: Int, val token: UUID)

/** Stores job inputs and manages claims. Archive writes and reads belong to the endpoint and worker. */
interface WebhookJobs {
    /** Insert once by vendor and delivery ID. A successful return means the job was accepted. */
    fun enqueue(job: WebhookJob)
    fun contains(vendor: String, deliveryId: String): Boolean
    fun claim(vendor: String, kind: JobKind): ClaimedWebhook?
    fun complete(claim: ClaimedWebhook)
    fun fail(claim: ClaimedWebhook, terminal: Boolean, delaySeconds: Int, failure: String)
}

internal const val MAX_WEBHOOK_ATTEMPTS = 5

class WebhookWorker(
    private val handler: WebhookHandler,
    private val jobs: WebhookJobs,
    private val archive: ReadableWebhookArchive? = null
) {
    suspend fun runOnce(): Boolean {
        val job = runInterruptible(Dispatchers.IO) {
            val kind = if (handler.processingMode == ProcessingMode.ASYNC_PROJECTED) JobKind.PROJECTED else JobKind.ARCHIVE
            jobs.claim(handler.vendor, kind)
        } ?: return false

        try {
            runInterruptible(Dispatchers.IO) {
                val (vendor, id, input) = job.job
                val body = when (input) {
                    is JobInput.Projected -> input.body
                    JobInput.Archived -> checkNotNull(archive?.load(vendor, id)) {
                        "Archived delivery is unavailable"
                    }
                }
                handler.handle(Delivery(id, body))
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
