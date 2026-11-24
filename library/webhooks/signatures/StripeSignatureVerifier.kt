package webhooks.signatures

import java.io.IOException
import java.time.Clock
import java.time.Instant
import webhooks.SignatureVerification
import webhooks.WebhookSignatureVerifier

/**
 * Verifies Stripe-Signature v1 against the timestamp and exact request bytes.
 * [eventId] reads the top-level ID from the authenticated Stripe JSON using the host's JSON parser.
 */
class StripeSignatureVerifier(
    endpointSecret: String,
    private val clock: Clock = Clock.systemUTC(),
    private val toleranceSeconds: Long = 300,
    private val eventId: (ByteArray) -> String?
) : WebhookSignatureVerifier {
    private val hmac = HmacSha256(endpointSecret.toByteArray(Charsets.UTF_8))

    init {
        require(toleranceSeconds > 0) { "Stripe signature tolerance must be positive." }
    }

    override fun verify(headers: Map<String, List<String>>, rawBody: ByteArray): SignatureVerification {
        val header = headers.singleHeader("Stripe-Signature") ?: return SignatureVerification.InvalidSignature
        val fields = header.split(',').map { it.trim().split('=', limit = 2) }
        val timestampText = fields.filter { it.first() == "t" }.singleOrNull()?.getOrNull(1)
            ?: return SignatureVerification.InvalidSignature
        val timestamp = timestampText.toLongOrNull() ?: return SignatureVerification.InvalidSignature
        if (timestamp !in 1..Instant.MAX.epochSecond || timestamp.toString() != timestampText) {
            return SignatureVerification.InvalidSignature
        }
        val ageSeconds = clock.instant().epochSecond - timestamp
        if (ageSeconds !in -toleranceSeconds..toleranceSeconds) return SignatureVerification.InvalidSignature

        val expected = hmac.digest(timestampText, '.', rawBody)
        var matched = false
        for (field in fields) {
            if (field.first() == "v1" && field.size == 2) {
                // Evaluate every candidate, including after a match during secret rotation.
                matched = matched or matchesSha256(expected, field[1])
            }
        }
        if (!matched) return SignatureVerification.InvalidSignature

        val id = try {
            eventId(rawBody)
        } catch (_: IllegalArgumentException) {
            null
        } catch (_: IOException) {
            null
        }
        return if (id.isNullOrBlank()) SignatureVerification.InvalidDelivery else SignatureVerification.Verified(id)
    }
}
