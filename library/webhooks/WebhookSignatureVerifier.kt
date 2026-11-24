package webhooks

/** Verify the unmodified request bytes and return an authenticated delivery ID for deduplication. */
fun interface WebhookSignatureVerifier {
    /** Header names are case-insensitive. All values are provided, including repeated fields. */
    fun verify(headers: Map<String, List<String>>, rawBody: ByteArray): SignatureVerification
}

sealed interface SignatureVerification {
    data class Verified(val deliveryId: String) : SignatureVerification
    data object InvalidSignature : SignatureVerification
    data object InvalidDelivery : SignatureVerification
}
