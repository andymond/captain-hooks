import java.util.UUID
import webhooks.Delivery
import webhooks.jdbc.JdbcDialect
import webhooks.archive.JdbcWebhookArchive
import webhooks.postgresConnectionFactory

internal fun testPostgresArchive() {
    val url = System.getenv("WEBHOOK_TEST_DATABASE_URL") ?: return
    val connect = postgresConnectionFactory(
        url, System.getenv("WEBHOOK_DATABASE_USER"), System.getenv("WEBHOOK_DATABASE_PASSWORD")
    )
    val archive = JdbcWebhookArchive(JdbcDialect.POSTGRESQL, connect)
    archive.verifySchema()
    val vendor = "archive-test"
    val otherVendor = "archive-test-other"
    val id = UUID.randomUUID().toString()
    val original = byteArrayOf(0, -1, 10)
    val other = byteArrayOf(4, 5)
    try {
        archive.save(vendor, Delivery(id, original))
        archive.save(vendor, Delivery(id, byteArrayOf(42)))
        archive.save(otherVendor, Delivery(id, other))
        connect().use { connection ->
            connection.prepareStatement(
                """
                SELECT vendor, body, received_at FROM webhook_deliveries
                WHERE delivery_id = ? AND vendor IN (?, ?)
                ORDER BY vendor
                """.trimIndent()
            ).use { query ->
                query.setString(1, id)
                query.setString(2, vendor)
                query.setString(3, otherVendor)
                query.executeQuery().use { rows ->
                    check(rows.next() && rows.getString(1) == vendor)
                    check(rows.getBytes(2).contentEquals(original) && rows.getTimestamp(3) != null)
                    check(rows.next() && rows.getString(1) == otherVendor)
                    check(rows.getBytes(2).contentEquals(other) && rows.getTimestamp(3) != null)
                    check(!rows.next())
                }
            }
        }
    } finally {
        connect().use { connection ->
            connection.prepareStatement(
                "DELETE FROM webhook_deliveries WHERE delivery_id = ? AND vendor IN (?, ?)"
            ).use { delete ->
                delete.setString(1, id)
                delete.setString(2, vendor)
                delete.setString(3, otherVendor)
                delete.executeUpdate()
            }
        }
    }
    println("PASS PostgreSQL archive: exact bytes, vendor isolation, and idempotent inserts")
}
