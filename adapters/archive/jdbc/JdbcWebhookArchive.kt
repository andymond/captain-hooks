package webhooks.archive

import java.sql.Connection
import java.sql.SQLException
import javax.sql.DataSource
import webhooks.Delivery
import webhooks.ReadableWebhookArchive
import webhooks.jdbc.JdbcDialect
import webhooks.jdbc.isDuplicate
import webhooks.jdbc.keyCollation

class JdbcWebhookArchive(
    private val dialect: JdbcDialect,
    private val connect: () -> Connection
) : ReadableWebhookArchive {
    constructor(dataSource: DataSource, dialect: JdbcDialect) : this(dialect, { dataSource.connection })

    fun verifySchema() {
        connect().use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(
                    "SELECT vendor, delivery_id, body, received_at FROM webhook_deliveries WHERE 1 = 0"
                ).close()
            }
        }
    }

    /** Run at setup time or use the same schema in your application's migration. */
    fun createSchema() {
        connect().use { connection ->
            connection.createStatement().use { statement ->
                statement.executeUpdate(
                    """
                    CREATE TABLE IF NOT EXISTS webhook_deliveries (
                        vendor varchar(63)${dialect.keyCollation} NOT NULL,
                        delivery_id varchar(128)${dialect.keyCollation} NOT NULL,
                        body ${dialect.bodyType} NOT NULL,
                        received_at ${dialect.timeType} NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (vendor, delivery_id)
                    )
                    """.trimIndent()
                )
            }
            if (!connection.autoCommit) connection.commit()
        }
    }

    override fun save(vendor: String, delivery: Delivery) {
        try {
            connect().use { connection ->
                connection.prepareStatement(
                    "INSERT INTO webhook_deliveries (vendor, delivery_id, body) VALUES (?, ?, ?)"
                ).use { statement ->
                    statement.setString(1, vendor)
                    statement.setString(2, delivery.id)
                    statement.setBytes(3, delivery.body)
                    statement.executeUpdate()
                }
                if (!connection.autoCommit) connection.commit()
            }
        } catch (failure: SQLException) {
            if (!dialect.isDuplicate(failure) || load(vendor, delivery.id) == null) throw failure
        }
    }

    override fun load(vendor: String, deliveryId: String): ByteArray? =
        connect().use { connection -> readArchive(connection, vendor, deliveryId) }
}

private fun readArchive(connection: Connection, vendor: String, deliveryId: String): ByteArray? =
    connection.prepareStatement(
        "SELECT body FROM webhook_deliveries WHERE vendor = ? AND delivery_id = ?"
    ).use { statement ->
        statement.setString(1, vendor)
        statement.setString(2, deliveryId)
        statement.executeQuery().use { if (it.next()) it.getBytes(1) else null }
    }
