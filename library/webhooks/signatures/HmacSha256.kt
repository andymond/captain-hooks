package webhooks.signatures

import java.security.MessageDigest
import java.util.HexFormat
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

internal class HmacSha256(secret: ByteArray) {
    private val key = SecretKeySpec(secret, "HmacSHA256")

    fun digest(prefix: String, separator: Char, body: ByteArray): ByteArray {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(key)
        mac.update(prefix.toByteArray(Charsets.UTF_8))
        mac.update(separator.code.toByte())
        return mac.doFinal(body)
    }
}

private val sha256Hex = Regex("[0-9a-fA-F]{64}")

internal fun matchesSha256(expected: ByteArray, hex: String): Boolean {
    if (!sha256Hex.matches(hex)) return false
    // The first argument is the fixed-size computed digest. Compare bytes without an early mismatch exit.
    return MessageDigest.isEqual(expected, HexFormat.of().parseHex(hex))
}

internal fun Map<String, List<String>>.singleHeader(name: String): String? =
    entries.asSequence().filter { it.key.equals(name, ignoreCase = true) }.flatMap { it.value.asSequence() }.singleOrNull()
