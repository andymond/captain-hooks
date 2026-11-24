import java.util.UUID
import kotlinx.coroutines.runBlocking
import webhooks.*

internal fun testLibrary() = withTempDirectory { directory ->
    val secret = "library-test".toByteArray()
    val body = byteArrayOf(0, -1, 1)
    val archived = mutableMapOf<Pair<String, String>, ByteArray>()
    val archive = object : ReadableWebhookArchive {
        override fun save(vendor: String, delivery: Delivery) {
            archived.putIfAbsent(vendor to delivery.id, delivery.body.copyOf())
        }

        override fun load(vendor: String, deliveryId: String) = archived[vendor to deliveryId]
    }
    var pending: ClaimedWebhook? = null
    var held = false
    var queueAvailable = false
    var completed = false
    var failureCount = 0
    val jobs = object : WebhookJobs {
        override fun enqueue(job: WebhookJob) {
            check(queueAvailable)
            check(job.input == JobInput.Archived)
            pending = ClaimedWebhook(job, 1, UUID.randomUUID())
        }

        override fun contains(vendor: String, deliveryId: String) = pending?.job?.deliveryId == deliveryId

        override fun claim(vendor: String, kind: JobKind): ClaimedWebhook? {
            if (held || completed) return null
            return pending?.also { held = true }
        }

        override fun complete(claim: ClaimedWebhook) { completed = true }

        override fun fail(claim: ClaimedWebhook, terminal: Boolean, delaySeconds: Int, failure: String) {
            check(!terminal)
            failureCount++
            held = false
        }
    }
    var handled = 0
    val handler = object : WebhookHandler {
        override val vendor = "library_test"
        override val processingMode = ProcessingMode.ASYNC_ARCHIVE
        override val persistWebhooks = true
        override fun handle(delivery: Delivery) {
            check(delivery.body.contentEquals(body))
            handled++
        }
    }
    val endpoint = WebhookEndpoint(handler, secret, directory, archive, jobs)
    val signature = sign(secret, "external-1", body)
    fun send(vararg ids: String) = runBlocking {
        endpoint.handleAsync("POST", ids.toList(), listOf(signature), body.inputStream())
    }
    check(send("external-1", "external-1").status == 400)
    check(endpoint.handle("GET", emptyList(), emptyList(), body.inputStream()).headers["Allow"] == "POST")
    val unavailable = send("external-1")
    check(unavailable.status == 503 && unavailable.headers["Retry-After"] == "1")
    check(archived.size == 1 && pending == null)
    queueAvailable = true
    check(send("external-1").status == 204)
    check(send("external-1").status == 204)
    check(pending?.job?.input == JobInput.Archived && handled == 0)
    archived.clear()
    val worker = WebhookWorker(handler, jobs, archive)
    check(runBlocking { worker.runOnce() } && failureCount == 1 && handled == 0)
    archive.save(handler.vendor, Delivery("external-1", body))
    check(runBlocking { worker.runOnce() } && completed && handled == 1)
    println("PASS library HTTP entry points and async external archive recovery")
}
