package webhooks.signatures

import java.security.MessageDigest
import java.util.HexFormat
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import webhooks.SignatureVerification
import webhooks.WebhookSignatureVerifier
import webhooks.deliveryIdFormat

/** Verifies the CLI demo's delivery ID and raw-body signature. */
class CaptainHooksSignatureVerifier(secret: ByteArray) : WebhookSignatureVerifier {
    private val key = SecretKeySpec(secret, "HmacSHA256")

    override fun verify(headers: Map<String, List<String>>, rawBody: ByteArray): SignatureVerification {
        val id = headers.singleHeader("X-Webhook-Delivery")
        if (id == null || !deliveryIdFormat.matches(id)) return SignatureVerification.InvalidDelivery
        val signature = headers.singleHeader("X-Webhook-Signature")
        if (signature == null || !signatureFormat.matches(signature)) return SignatureVerification.InvalidSignature

        val mac = Mac.getInstance("HmacSHA256")
        mac.init(key)
        mac.update(id.toByteArray(Charsets.UTF_8))
        mac.update(10.toByte())
        val expected = mac.doFinal(rawBody)
        val supplied = HexFormat.of().parseHex(signature.removePrefix("sha256="))
        // Both arguments contain 32 bytes. The comparison must inspect every byte.
        return if (MessageDigest.isEqual(expected, supplied)) {
            SignatureVerification.Verified(id)
        } else SignatureVerification.InvalidSignature
    }
}

private val signatureFormat = Regex("sha256=[0-9a-fA-F]{64}")

private fun Map<String, List<String>>.singleHeader(name: String): String? =
    entries.asSequence().filter { it.key.equals(name, ignoreCase = true) }.flatMap { it.value.asSequence() }.singleOrNull()
