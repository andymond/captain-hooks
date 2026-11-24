package webhooks.incident_alerts

import webhooks.Delivery
import webhooks.ProcessingMode
import webhooks.WebhookHandler

class Handler : WebhookHandler {
    override val vendor = "incident_alerts"
    override val persistWebhooks = true
    override val processingMode = ProcessingMode.ASYNC_ARCHIVE

    override fun handle(delivery: Delivery) {
        // A real incident integration can attach the original alert to its incident record.
        println("Processed incident alert ${delivery.id} (${delivery.body.size} original bytes)")
    }
}
