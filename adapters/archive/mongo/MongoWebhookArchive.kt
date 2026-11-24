package webhooks.archive

import com.mongodb.ErrorCategory
import com.mongodb.MongoWriteException
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters.eq
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates.combine
import com.mongodb.client.model.Updates.setOnInsert
import org.bson.Document
import org.bson.types.Binary
import java.util.Date
import webhooks.Delivery
import webhooks.ReadableWebhookArchive
import webhooks.adapters.archiveKey

/** Uses the application's collection and its configured write concern. */
class MongoWebhookArchive(private val collection: MongoCollection<Document>) : ReadableWebhookArchive {
    init {
        require(collection.writeConcern.isAcknowledged) { "MongoDB archive writes must be acknowledged." }
    }

    override fun save(vendor: String, delivery: Delivery) {
        try {
            collection.updateOne(
                eq("_id", archiveKey(vendor, delivery.id)),
                combine(
                    setOnInsert("vendor", vendor),
                    setOnInsert("deliveryId", delivery.id),
                    setOnInsert("body", Binary(delivery.body)),
                    setOnInsert("receivedAt", Date())
                ),
                UpdateOptions().upsert(true)
            )
        } catch (failure: MongoWriteException) {
            if (failure.error.category != ErrorCategory.DUPLICATE_KEY || load(vendor, delivery.id) == null) {
                throw failure
            }
        }
    }

    override fun load(vendor: String, deliveryId: String): ByteArray? =
        collection.find(eq("_id", archiveKey(vendor, deliveryId))).first()?.get("body", Binary::class.java)?.data
}
