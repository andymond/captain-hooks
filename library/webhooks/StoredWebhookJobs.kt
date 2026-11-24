package webhooks

import java.time.Clock
import java.util.UUID

internal enum class JobInput { PROJECTED, ARCHIVE }

internal data class JobRecord(
    val vendor: String,
    val id: String,
    val input: JobInput,
    val body: ByteArray?,
    val revision: String = UUID.randomUUID().toString(),
    val attempt: Int = 0,
    val due: Long?,
    val lease: Boolean = false,
    val failure: String? = null
)

internal interface JobRecords {
    fun insert(record: JobRecord)
    fun get(vendor: String, id: String): JobRecord?
    fun due(vendor: String, input: JobInput, now: Long): List<JobRecord>
    fun replace(previous: JobRecord, next: JobRecord): Boolean
}

internal class StoredWebhookJobs(
    private val records: JobRecords,
    private val clock: Clock = Clock.systemUTC()
) : WebhookJobs {
    override val supportsExternalArchive = true
    override val supportsEmbeddedArchive = false

    override fun enqueue(vendor: String, delivery: Delivery, projected: ByteArray?, archive: Boolean) {
        require(!archive) { "Configure an independent archive with this queue." }
        records.insert(JobRecord(vendor, delivery.id, JobInput.PROJECTED, requireNotNull(projected), due = clock.millis()))
    }

    override fun enqueueArchiveReference(vendor: String, deliveryId: String) {
        records.insert(JobRecord(vendor, deliveryId, JobInput.ARCHIVE, null, due = clock.millis()))
    }

    override fun contains(vendor: String, id: String) = records.get(vendor, id) != null

    override fun claim(vendor: String, projected: Boolean): ClaimedWebhook? {
        val now = clock.millis()
        val input = if (projected) JobInput.PROJECTED else JobInput.ARCHIVE
        for (record in records.due(vendor, input, now)) {
            if (record.input != input || record.due == null || record.due > now) continue
            val token = UUID.randomUUID()
            if (record.attempt >= MAX_WEBHOOK_ATTEMPTS) {
                records.replace(record, record.copy(revision = token.toString(), due = null, lease = false, failure = "LeaseExpired"))
                continue
            }
            val claim = record.copy(revision = token.toString(), attempt = record.attempt + 1, due = now + LEASE_MS, lease = true)
            if (records.replace(record, claim)) {
                return ClaimedWebhook(vendor, Delivery(record.id, record.body ?: byteArrayOf()), claim.attempt, token, input == JobInput.ARCHIVE)
            }
        }
        return null
    }

    override fun complete(job: ClaimedWebhook) = update(job) { it.copy(body = null, due = null, failure = null) }

    override fun fail(job: ClaimedWebhook, terminal: Boolean, delaySeconds: Int, failure: String) =
        update(job) { it.copy(due = if (terminal) null else clock.millis() + delaySeconds * 1000L, failure = failure) }

    private fun update(job: ClaimedWebhook, transform: (JobRecord) -> JobRecord) {
        val current = records.get(job.vendor, job.delivery.id) ?: return
        if (!current.lease || current.revision != job.token.toString() || (current.due ?: 0) <= clock.millis()) return
        records.replace(current, transform(current).copy(revision = UUID.randomUUID().toString(), lease = false))
    }

    private companion object {
        const val LEASE_MS = 300_000L
    }
}
