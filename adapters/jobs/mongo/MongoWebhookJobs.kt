package webhooks.jobs

import com.mongodb.ErrorCategory
import com.mongodb.MongoWriteException
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters.and
import com.mongodb.client.model.Filters.eq
import com.mongodb.client.model.Filters.lte
import com.mongodb.client.model.Indexes.ascending
import org.bson.Document
import org.bson.types.Binary
import webhooks.JobKind
import webhooks.WebhookJobs
import webhooks.archiveKey

/** Durable queue backed by an application-managed MongoDB collection. */
class MongoWebhookJobs private constructor(private val records: MongoJobRecords) :
    WebhookJobs by StoredWebhookJobs(records) {
    constructor(collection: MongoCollection<Document>) : this(MongoJobRecords(collection))

    fun createIndexes() = records.createIndexes()
}

private class MongoJobRecords(private val collection: MongoCollection<Document>) : JobRecords {
    init {
        require(collection.writeConcern.isAcknowledged)
    }

    fun createIndexes(): String = collection.createIndex(ascending("vendor", "input", "due"))

    override fun insert(record: JobRecord) {
        try {
            collection.insertOne(record.document())
        } catch (failure: MongoWriteException) {
            if (failure.error.category != ErrorCategory.DUPLICATE_KEY || get(record.vendor, record.id) == null) {
                throw failure
            }
        }
    }

    override fun get(vendor: String, id: String): JobRecord? =
        collection.find(eq("_id", archiveKey(vendor, id))).first()?.job()

    override fun due(vendor: String, input: JobKind, now: Long): List<JobRecord> =
        collection.find(and(eq("vendor", vendor), eq("input", input.name), lte("due", now)))
            .sort(ascending("due")).limit(32).map { it.job() }.into(mutableListOf())

    override fun replace(previous: JobRecord, next: JobRecord): Boolean =
        collection.replaceOne(
            and(eq("_id", archiveKey(previous.vendor, previous.id)), eq("revision", previous.revision)),
            next.document()
        ).matchedCount == 1L

    private fun JobRecord.document() = Document("_id", archiveKey(vendor, id))
        .append("vendor", vendor).append("id", id).append("input", input.name)
        .append("body", body?.let { Binary(it) }).append("revision", revision)
        .append("attempt", attempt).append("due", due).append("leased", lease).append("failure", failure)

    private fun Document.job() = JobRecord(
        getString("vendor"), getString("id"), JobKind.valueOf(getString("input")),
        get("body", Binary::class.java)?.data, getString("revision"), getInteger("attempt"),
        getLong("due"), getBoolean("leased"), getString("failure")
    )
}
