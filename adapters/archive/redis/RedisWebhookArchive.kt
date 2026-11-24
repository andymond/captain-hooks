package webhooks.archive

import redis.clients.jedis.UnifiedJedis
import redis.clients.jedis.params.SetParams
import webhooks.Delivery
import webhooks.ReadableWebhookArchive
import webhooks.archiveKey

/** Uses the application's configured Redis client. Configure durable, non-evicting storage for archives. */
class RedisWebhookArchive(
    private val redis: UnifiedJedis,
    private val keyPrefix: String = "captain-hooks:archive:"
) : ReadableWebhookArchive {
    override fun save(vendor: String, delivery: Delivery) {
        redis.set(key(vendor, delivery.id), delivery.body, SetParams().nx())
    }

    override fun load(vendor: String, deliveryId: String): ByteArray? = redis.get(key(vendor, deliveryId))

    private fun key(vendor: String, id: String) = (keyPrefix + archiveKey(vendor, id)).toByteArray()
}
