import webhooks.Delivery
import webhooks.DeliveryReceipts
import webhooks.ReceiptResult
import webhooks.WebhookEndpoint
import webhooks.WebhookHandler

internal fun testInjectedReceipts() {
    val secret = byteArrayOf(1)
    val body = byteArrayOf(2)
    val completed = mutableSetOf<Pair<String, String>>()
    var calls = 0
    var storeAvailable = false
    val receipts = DeliveryReceipts { vendor, id, handle ->
        check(storeAvailable)
        val key = vendor to id
        when {
            id == "busy" -> ReceiptResult.BUSY
            key in completed -> ReceiptResult.DUPLICATE
            else -> {
                handle()
                completed += key
                ReceiptResult.PROCESSED
            }
        }
    }

    fun endpoint(name: String): WebhookEndpoint {
        val handler = object : WebhookHandler {
            override val vendor = name
            override fun handle(delivery: Delivery) {
                calls++
                if (delivery.id == "retry" && calls == 1) error("handler unavailable")
            }
        }
        check(runCatching { WebhookEndpoint(handler, secret) }.isFailure)
        return WebhookEndpoint(handler, secret, receipts = receipts)
    }

    fun send(route: WebhookEndpoint, id: String, signed: Boolean = true) =
        route.receive("POST", route.route, id, sign(secret, id, body).takeIf { signed }, body.inputStream())

    val one = endpoint("first")
    val two = endpoint("second")
    check(send(one, "retry", signed = false) == 401)
    check(send(one, "retry") == 503 && calls == 0)
    storeAvailable = true
    check(send(one, "busy") == 503 && calls == 0)
    check(send(one, "retry") == 500 && completed.isEmpty())
    check(send(one, "retry") == 204)
    check(send(one, "retry") == 204 && calls == 2)
    check(send(two, "retry") == 204 && calls == 3)
    println("PASS injected delivery receipts, vendor isolation, and retry semantics")
}
