package webhooks

/** Saves the first verified body for a vendor and delivery ID. Repeated writes must be idempotent. */
fun interface WebhookArchive {
    fun save(vendor: String, delivery: Delivery)
}

/** An archive that can supply the original body to an async worker. */
interface ReadableWebhookArchive : WebhookArchive {
    fun load(vendor: String, deliveryId: String): ByteArray?
}
