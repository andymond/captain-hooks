package webhooks.signatures

import webhooks.SignatureVerification
import webhooks.WebhookSignatureVerifier
import webhooks.deliveryIdFormat

/** Verifies the CLI demo's delivery ID and raw-body signature. */
class CaptainHooksSignatureVerifier(secret: ByteArray) : WebhookSignatureVerifier {
    private val hmac = HmacSha256(secret)

    override fun verify(headers: Map<String, List<String>>, rawBody: ByteArray): SignatureVerification {
        val id = headers.singleHeader("X-Webhook-Delivery")
        if (id == null || !deliveryIdFormat.matches(id)) return SignatureVerification.InvalidDelivery
        val signature = headers.singleHeader("X-Webhook-Signature")
        if (signature == null || !signature.startsWith("sha256=")) return SignatureVerification.InvalidSignature
        return if (matchesSha256(hmac.digest(id, '\n', rawBody), signature.removePrefix("sha256="))) {
            SignatureVerification.Verified(id)
        } else SignatureVerification.InvalidSignature
    }
}
