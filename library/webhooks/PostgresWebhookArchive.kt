package webhooks

import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties


public class PostgresWebhookArchive(
    private val connect: () -> Connection
) : ReadableWebhookArchive {
    fun verifySchema() {
        connect().use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(
                    "SELECT vendor, delivery_id, body, received_at FROM webhook_deliveries WHERE FALSE"
                ).close()
            }
        }
    }

    override fun load(vendor: String, deliveryId: String): ByteArray? =
        connect().use { connection -> readArchive(connection, vendor, deliveryId) }

    override fun save(vendor: String, delivery: Delivery) {
        connect().use { connection -> insertArchive(connection, vendor, delivery) }
    }
}

internal fun insertArchive(connection: Connection, vendor: String, delivery: Delivery) {
    connection.prepareStatement(
        """
        INSERT INTO webhook_deliveries (vendor, delivery_id, body)
        VALUES (?, ?, ?)
        ON CONFLICT (vendor, delivery_id) DO NOTHING
        """.trimIndent()
    ).use { statement ->
        statement.setString(1, vendor)
        statement.setString(2, delivery.id)
        statement.setBytes(3, delivery.body)
        statement.executeUpdate()
    }
}

public fun postgresConnectionFactory(url: String, user: String?, password: String?): () -> Connection {
    val properties = Properties().apply {
        setProperty("connectTimeout", "5")
        setProperty("socketTimeout", "10")
        if (user != null) setProperty("user", user)
        if (password != null) setProperty("password", password)
    }
    return { DriverManager.getConnection(url, properties) }
}
