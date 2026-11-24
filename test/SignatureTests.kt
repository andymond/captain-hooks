import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.HexFormat
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import webhooks.*
import webhooks.signatures.StripeSignatureVerifier

internal fun testStripeSignatures() {
    val instant = Instant.ofEpochSecond(1_700_000_000)
    val clock = Clock.fixed(instant, ZoneOffset.UTC)
    val secret = "whsec_test"
    val payload = """{"id":"evt_demo","object":"event","data":{"object":{"id":"nested"}}}""".toByteArray()
    val invalidPayload = """{"object":"event"}""".toByteArray()
    var extracted = 0
    val verifier = StripeSignatureVerifier(secret, clock = clock) { body ->
        extracted++
        // Integration fixture. Real hosts use their configured JSON reader.
        "evt_demo".takeIf { body.contentEquals(payload) }
    }
    val calls = mutableListOf<String>()
    val processed = mutableSetOf<String>()
    val endpoint = WebhookEndpoint(object : WebhookHandler {
        override val vendor = "stripe"
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

    fun signed(timestamp: Long = instant.epochSecond, body: ByteArray = payload): String {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(secret.toByteArray(), "HmacSHA256"))
        val digest = mac.doFinal("$timestamp.".toByteArray() + body)
        return "t=$timestamp,v1=${HexFormat.of().formatHex(digest)}"
    }

    fun send(signature: String, body: ByteArray = payload, headerName: String = "stripe-signature") =
        endpoint.handle(
            "POST", mapOf(headerName to listOf(signature), "X-Webhook-Delivery" to listOf("unsigned")), body.inputStream()
        ).status

    val valid = signed()
    check(valid == "t=1700000000,v1=94cd90aeec6cb79362712a5737662f51e4702a2b6bb90471e378403360449863")
    check(send(valid, payload + 10.toByte()) == 401)
    check(send(signed(instant.epochSecond - 301)) == 401)
    check(send(signed(instant.epochSecond + 301)) == 401)
    check(send(valid.replace("v1=", "v0=")) == 401)
    check(send("$valid,t=${instant.epochSecond}") == 401)
    check(send(valid, headerName = "X-Webhook-Signature") == 401)
    check(endpoint.handle(
        "POST", mapOf("Stripe-Signature" to listOf(valid), "stripe-signature" to listOf(valid)), payload.inputStream()
    ).status == 401)
    check(extracted == 0 && calls.isEmpty())

    check(send(signed(body = invalidPayload), invalidPayload) == 400)
    check(send("t=${instant.epochSecond},v1=${"0".repeat(64)},${valid.substringAfter(',')}") == 204)
    check(send(signed(instant.epochSecond - 300)) == 204)
    check(calls == listOf("evt_demo"))
    println("PASS Stripe signatures, replay window, signed event IDs, and duplicate deliveries")
}
