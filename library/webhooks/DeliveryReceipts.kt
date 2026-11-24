package webhooks

/** Coordinates synchronous handling by vendor and delivery ID. */
fun interface DeliveryReceipts {
    /**
     * Run [handle] at most once concurrently for this key. Persist completion only after it succeeds.
     * Propagate failures so the sender can retry. Return [ReceiptResult.DUPLICATE] for completed work
     * and [ReceiptResult.BUSY] if another request owns the claim.
     */
    fun process(vendor: String, deliveryId: String, handle: () -> Unit): ReceiptResult
}

enum class ReceiptResult { PROCESSED, DUPLICATE, BUSY }
