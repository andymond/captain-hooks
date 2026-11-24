import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.runBlocking
import webhooks.*

internal fun testAsyncWebhooks() = withTempDirectory { directory ->
    fun runOnce(worker: WebhookWorker) = runBlocking { worker.runOnce() }
    val queue = TestJobStore()
    val originalBodies = mutableMapOf<Pair<String, String>, ByteArray>()
    val archive = object : ReadableWebhookArchive {
        override fun save(vendor: String, delivery: Delivery) {
            originalBodies.putIfAbsent(vendor to delivery.id, delivery.body.copyOf())
        }
        override fun load(vendor: String, deliveryId: String) = originalBodies[vendor to deliveryId]
    }
    val secret = "async-test".toByteArray()
    val incident = loadHandler("incident-alerts")
    val enter = CountDownLatch(1)
    val release = CountDownLatch(1)
    var incidentCalls = 0
    val slow = object : WebhookHandler by incident {
        override fun handle(delivery: Delivery) {
            incidentCalls++
            check(delivery.body.contentEquals("checkout is down".toByteArray()))
            enter.countDown()
            check(release.await(5, TimeUnit.SECONDS))
            incident.handle(delivery)
        }
    }
    check(runCatching { WebhookEndpoint(incident, secret, directory) }.exceptionOrNull() is IllegalArgumentException)
    val invalidArchive = object : WebhookHandler by incident {
        override val persistWebhooks = false
    }
    check(runCatching {
        WebhookEndpoint(invalidArchive, secret, directory, jobs = queue)
    }.exceptionOrNull() is IllegalArgumentException)
    val alertEndpoint = WebhookEndpoint(slow, secret, directory, archive, queue)
    val fulfillment = loadHandler("shipment-updates")
    val shipmentEndpoint = WebhookEndpoint(fulfillment, secret, directory, jobs = queue)

    fun send(endpoint: WebhookEndpoint, id: String, text: String, valid: Boolean = true): Int {
        val body = text.toByteArray()
        val signature = if (valid) sign(secret, id, body) else null
        return endpoint.receive("POST", endpoint.route, id, signature, body.inputStream())
    }

    check(send(alertEndpoint, "alert-1", "checkout is down", valid = false) == 401)
    check(queue.submissions.isEmpty())
    queue.unavailable = true
    check(send(alertEndpoint, "alert-1", "checkout is down") == 503)
    check(queue.submissions.isEmpty())
    queue.unavailable = false
    check(send(alertEndpoint, "alert-1", "checkout is down") == 204)
    check(incidentCalls == 0)
    val alert = queue.submissions.single()
    check(alert.input == JobInput.Archived && archive.load(alert.vendor, alert.deliveryId)?.decodeToString() == "checkout is down")
    WebhookWorker(slow, queue, archive).start().use {
        try {
            check(enter.await(5, TimeUnit.SECONDS))
            check(send(alertEndpoint, "alert-1", "checkout is down") == 204)
            check(queue.submissions.size == 1)
        } finally {
            release.countDown()
        }
        check(queue.alertCompleted.await(5, TimeUnit.SECONDS))
    }
    check(queue.completed == listOf("alert-1") && incidentCalls == 1)

    check(send(shipmentEndpoint, "shipment-1", "orderId=123") == 400)
    check(queue.submissions.size == 1)
    val input = "orderId=123&trackingNumber=ZX9&status=shipped&address=42+Private+Lane"
    check(send(shipmentEndpoint, "shipment-1", input) == 204)
    check(send(shipmentEndpoint, "shipment-1", "invalid retry") == 204)
    val shipment = queue.submissions.last()
    check(archive.load(shipment.vendor, shipment.deliveryId) == null)
    val projected = (shipment.input as JobInput.Projected).body
    check(projected.decodeToString() == "v1\norderId=123&trackingNumber=ZX9&status=shipped")
    check(runOnce(WebhookWorker(fulfillment, queue)))
    check(queue.completed == listOf("alert-1", "shipment-1"))

    val archivingFulfillment = object : ProjectedWebhookHandler by (fulfillment as ProjectedWebhookHandler) {
        override val persistWebhooks = true
    }
    val archivingEndpoint = WebhookEndpoint(archivingFulfillment, secret, directory, archive, queue)
    check(send(archivingEndpoint, "shipment-with-archive", input) == 204)
    check(archive.load(archivingFulfillment.vendor, "shipment-with-archive")?.decodeToString() == input)
    check((queue.submissions.last().input as JobInput.Projected).body.contentEquals(projected))
    check(runOnce(WebhookWorker(archivingFulfillment, queue)))

    queue.enqueue(WebhookJob(fulfillment.vendor, "unsupported", JobInput.Projected("v2".toByteArray())))
    check(runOnce(WebhookWorker(fulfillment, queue)))
    check(queue.failures.last() == Triple("unsupported", true, 1))

    var attempts = 0
    val failing = object : WebhookHandler by fulfillment {
        override fun handle(delivery: Delivery) {
            attempts++
            if (attempts == 1 || delivery.id == "terminal") error("downstream unavailable")
            fulfillment.handle(delivery)
        }
    }
    val worker = WebhookWorker(failing, queue)
    check(send(shipmentEndpoint, "retry", input) == 204)
    check(runOnce(worker))
    check(queue.failures.last() == Triple("retry", false, 1))
    check(runOnce(worker))
    check(queue.completed.last() == "retry")
    check(send(shipmentEndpoint, "terminal", input) == 204)
    repeat(5) { check(runOnce(worker)) }
    check(queue.failures.last() == Triple("terminal", true, 16))
    check(!runOnce(worker))
    val cancelled = object : WebhookHandler by fulfillment {
        override fun handle(delivery: Delivery) = throw java.util.concurrent.CancellationException()
    }
    check(send(shipmentEndpoint, "cancelled", input) == 204)
    val beforeCancellation = queue.failures.size
    check(runCatching { runOnce(WebhookWorker(cancelled, queue)) }.exceptionOrNull() is
        java.util.concurrent.CancellationException)
    check(queue.failures.size == beforeCancellation && "cancelled" !in queue.completed)

    val shutdownEntered = CountDownLatch(1)
    val interrupted = CountDownLatch(1)
    val blocked = object : WebhookHandler by fulfillment {
        override fun handle(delivery: Delivery) {
            shutdownEntered.countDown()
            try {
                CountDownLatch(1).await(5, TimeUnit.SECONDS)
            } catch (failure: InterruptedException) {
                interrupted.countDown()
                throw failure
            }
        }
    }
    check(send(shipmentEndpoint, "shutdown", input) == 204)
    WebhookWorker(blocked, queue).start().use {
        check(shutdownEntered.await(5, TimeUnit.SECONDS))
    }
    check(interrupted.await(1, TimeUnit.SECONDS))
    check(queue.failures.size == beforeCancellation && "shutdown" !in queue.completed)

    println("PASS async incident and shipment examples, fast acknowledgment, duplicates, and failure recovery")
}

private class TestJobStore : WebhookJobs {
    val submissions = mutableListOf<WebhookJob>()
    val completed = mutableListOf<String>()
    val alertCompleted = CountDownLatch(1)
    val failures = mutableListOf<Triple<String, Boolean, Int>>()
    var unavailable = false
    private val pending = ArrayDeque<Pair<WebhookJob, Int>>()

    @Synchronized
    override fun enqueue(job: WebhookJob) {
        check(!unavailable)
        if (contains(job.vendor, job.deliveryId)) return
        submissions += job
        pending.addLast(job to 1)
    }

    @Synchronized
    override fun contains(vendor: String, deliveryId: String): Boolean =
        submissions.any { it.vendor == vendor && it.deliveryId == deliveryId }

    @Synchronized
    override fun claim(vendor: String, kind: JobKind): ClaimedWebhook? {
        val index = pending.indexOfFirst { it.first.vendor == vendor && it.first.input.kind == kind }
        if (index == -1) return null
        val (job, attempt) = pending.removeAt(index)
        return ClaimedWebhook(job, attempt, java.util.UUID.randomUUID())
    }

    @Synchronized
    override fun complete(claim: ClaimedWebhook) {
        completed += claim.job.deliveryId
        if (claim.job.deliveryId == "alert-1") alertCompleted.countDown()
    }

    @Synchronized
    override fun fail(claim: ClaimedWebhook, terminal: Boolean, delaySeconds: Int, failure: String) {
        failures += Triple(claim.job.deliveryId, terminal, delaySeconds)
        if (!terminal) pending.addLast(claim.job to (claim.attempt + 1))
    }
}
