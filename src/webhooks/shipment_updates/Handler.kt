package webhooks.shipment_updates

import java.net.URLDecoder
import java.net.URLEncoder
import webhooks.Delivery
import webhooks.PermanentWebhookFailure
import webhooks.ProjectedWebhookHandler

class Handler : ProjectedWebhookHandler {
    override val vendor = "shipment_updates"

    override fun project(delivery: Delivery): ByteArray {
        val fields = shipmentFields(delivery.body.decodeToString())
        val encoded = fields.joinToString("&") { (name, value) ->
            "$name=${URLEncoder.encode(value, Charsets.UTF_8)}"
        }
        return "v1\n$encoded".toByteArray()
    }

    override fun handle(delivery: Delivery) {
        val payload = delivery.body.decodeToString()
        if (!payload.startsWith("v1\n")) throw PermanentWebhookFailure("Unsupported shipment payload version")
        val fields = shipmentFields(payload.removePrefix("v1\n")).toMap()
        println("Processed shipment ${delivery.id} for order ${fields.getValue("orderId")} " +
            "with tracking ${fields.getValue("trackingNumber")} and status ${fields.getValue("status")}")
    }

    private fun shipmentFields(payload: String): List<Pair<String, String>> {
        val fields = payload.split('&').map { field ->
            URLDecoder.decode(field.substringBefore('='), Charsets.UTF_8) to field.substringAfter('=', "")
        }
        return listOf("orderId", "trackingNumber", "status").map { name ->
            val encoded = requireNotNull(fields.singleOrNull { it.first == name }?.second)
            val value = URLDecoder.decode(encoded, Charsets.UTF_8)
            require(value.matches(Regex("[A-Za-z0-9_-]{1,64}")))
            name to value
        }
    }
}
