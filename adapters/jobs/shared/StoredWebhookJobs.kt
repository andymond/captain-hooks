package webhooks.jobs

import java.time.Clock
import java.util.UUID
import webhooks.ClaimedWebhook
import webhooks.JobInput
import webhooks.JobKind
import webhooks.MAX_WEBHOOK_ATTEMPTS
import webhooks.WebhookJob
import webhooks.WebhookJobs

/** Persistent state shared by the bundled queue adapters. */
data class JobRecord(
    val vendor: String,
    val id: String,
    val input: JobKind,
    val body: ByteArray?,
    val revision: String = UUID.randomUUID().toString(),
    val attempt: Int = 0,
    val due: Long?,
    val lease: Boolean = false,
    val failure: String? = null
)

/** Persistence operations for queue adapters with atomic insert and compare-and-set. */
interface JobRecords {
    /** Insert only if no record exists for the vendor and delivery ID. */
    fun insert(record: JobRecord)
    fun get(vendor: String, id: String): JobRecord?
    fun due(vendor: String, input: JobKind, now: Long): List<JobRecord>
    /** Update only if the stored revision still matches [previous]. */
    fun replace(previous: JobRecord, next: JobRecord): Boolean
}

/** Implements claim and retry behavior on top of an adapter-specific record store. */
class StoredWebhookJobs(
    private val records: JobRecords,
    private val clock: Clock = Clock.systemUTC()
) : WebhookJobs {
    override fun enqueue(job: WebhookJob) {
        val body = (job.input as? JobInput.Projected)?.body
        records.insert(JobRecord(job.vendor, job.deliveryId, job.input.kind, body, due = clock.millis()))
    }

    override fun contains(vendor: String, deliveryId: String) = records.get(vendor, deliveryId) != null

    override fun claim(vendor: String, kind: JobKind): ClaimedWebhook? {
        val now = clock.millis()
        for (record in records.due(vendor, kind, now)) {
            if (record.input != kind || record.due == null || record.due > now) continue
            val token = UUID.randomUUID()
            if (record.attempt >= MAX_WEBHOOK_ATTEMPTS) {
                records.replace(record, record.copy(revision = token.toString(), due = null, lease = false, failure = "LeaseExpired"))
                continue
            }
            val claim = record.copy(revision = token.toString(), attempt = record.attempt + 1, due = now + LEASE_MS, lease = true)
            if (records.replace(record, claim)) {
                val input = when (kind) {
                    JobKind.PROJECTED -> JobInput.Projected(checkNotNull(record.body))
                    JobKind.ARCHIVE -> JobInput.Archived
                }
                return ClaimedWebhook(WebhookJob(vendor, record.id, input), claim.attempt, token)
            }
        }
        return null
    }

    override fun complete(claim: ClaimedWebhook) = update(claim) { it.copy(body = null, due = null, failure = null) }

    override fun fail(claim: ClaimedWebhook, terminal: Boolean, delaySeconds: Int, failure: String) =
        update(claim) { it.copy(due = if (terminal) null else clock.millis() + delaySeconds * 1000L, failure = failure) }

    private fun update(job: ClaimedWebhook, transform: (JobRecord) -> JobRecord) {
        val current = records.get(job.job.vendor, job.job.deliveryId) ?: return
        if (!current.lease || current.revision != job.token.toString() || (current.due ?: 0) <= clock.millis()) return
        records.replace(current, transform(current).copy(revision = UUID.randomUUID().toString(), lease = false))
    }

    private companion object {
        const val LEASE_MS = 300_000L
    }
}
