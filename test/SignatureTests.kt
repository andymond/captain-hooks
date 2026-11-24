import webhooks.Delivery
import webhooks.DeliveryReceipts
import webhooks.ReceiptResult
import webhooks.SignatureVerification
import webhooks.WebhookEndpoint
import webhooks.WebhookHandler
import webhooks.WebhookSignatureVerifier

internal fun testPluggableSignatures() {
    val payload = byteArrayOf(1, -1, 2)
    val verifier = WebhookSignatureVerifier { headers, body ->
        when {
            headers["Provider-Token"] != listOf("fixture") -> SignatureVerification.InvalidSignature
            !body.contentEquals(payload) -> SignatureVerification.InvalidDelivery
            else -> {
                body.fill(0)
                SignatureVerification.Verified("verified_id")
            }
        }
    }
    val calls = mutableListOf<String>()
    val processed = mutableSetOf<String>()
    val endpoint = WebhookEndpoint(object : WebhookHandler {
        override val vendor = "provider"
        override fun handle(delivery: Delivery) {
            check(delivery.body.contentEquals(payload))
            calls += delivery.id
        }
    }, verifier = verifier, receipts = DeliveryReceipts { _, id, handle ->
        if (id in processed) ReceiptResult.DUPLICATE else {
            handle()
            processed += id
            ReceiptResult.PROCESSED
        }
    })
    fun send(tokens: List<String>, body: ByteArray = payload) = endpoint.handle(
        "POST", mapOf("Provider-Token" to tokens, "X-Webhook-Delivery" to listOf("unsigned")), body.inputStream()
    ).status

    check(send(emptyList()) == 401)
    check(send(listOf("fixture", "fixture")) == 401)
    check(send(listOf("fixture"), byteArrayOf(1)) == 400)
    check(calls.isEmpty())
    check(send(listOf("fixture")) == 204)
    check(send(listOf("fixture")) == 204)
    check(calls == listOf("verified_id"))
    println("PASS pluggable signatures, raw body preservation, and verified delivery IDs")
}
