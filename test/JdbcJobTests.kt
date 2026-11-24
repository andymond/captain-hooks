import java.util.UUID
import java.util.concurrent.CompletableFuture
import webhooks.*
import webhooks.jdbc.JdbcDialect
import webhooks.jobs.JdbcWebhookJobs

internal fun testJdbcJobs() {
    val url = System.getenv("WEBHOOK_TEST_DATABASE_URL") ?: return
    val connect = postgresConnectionFactory(
        url, System.getenv("WEBHOOK_DATABASE_USER"), System.getenv("WEBHOOK_DATABASE_PASSWORD")
    )
    val queue = JdbcWebhookJobs(JdbcDialect.POSTGRESQL, connect)
    queue.createSchema()
    val vendor = "job-test-${UUID.randomUUID()}"
    val body = byteArrayOf(0, -1, 10)
    val job = WebhookJob(vendor, "shipment", JobInput.Projected(body))
    try {
        queue.enqueue(job)
        queue.enqueue(job.copy(input = JobInput.Projected(byteArrayOf(9))))
        val first = CompletableFuture.supplyAsync { queue.claim(vendor, JobKind.PROJECTED) }
        val second = CompletableFuture.supplyAsync { queue.claim(vendor, JobKind.PROJECTED) }
        val claim = listOfNotNull(first.join(), second.join()).single()
        check((claim.job.input as JobInput.Projected).body.contentEquals(body))
        queue.complete(claim)
        check(queue.contains(vendor, job.deliveryId))
        check(queue.claim(vendor, JobKind.PROJECTED) == null)
        val reference = WebhookJob(vendor, "incident", JobInput.Archived)
        queue.enqueue(reference)
        val next = checkNotNull(queue.claim(vendor, JobKind.ARCHIVE))
        check(next.job == reference)
        queue.fail(next, terminal = true, delaySeconds = 0, failure = "TestFailure")
        check(queue.claim(vendor, JobKind.ARCHIVE) == null)
    } finally {
        connect().use { connection ->
            connection.prepareStatement("DELETE FROM captain_webhook_jobs WHERE vendor = ?").use {
                it.setString(1, vendor)
                it.executeUpdate()
            }
        }
    }
    println("PASS JDBC job storage: exclusive claims, exact bytes, references, and terminal jobs")
}
