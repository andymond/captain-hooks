package webhooks

import java.sql.Connection
import java.sql.SQLException
import javax.sql.DataSource

/** SQL archive formats supported by the bundled schema initializer. */
enum class ArchiveSqlDialect(internal val bodyType: String, internal val timeType: String) {
    POSTGRESQL("bytea", "timestamptz"),
    MYSQL("longblob", "timestamp"),
    MARIADB("longblob", "timestamp"),
    H2("blob", "timestamp with time zone"),
    SQLITE("blob", "timestamp")
}

class JdbcWebhookArchive(
    private val dataSource: DataSource,
    private val dialect: ArchiveSqlDialect
) : ReadableWebhookArchive {
    /** Run at setup time or use the same schema in your application's migration. */
    fun createSchema() {
        val keyCollation = when (dialect) {
            ArchiveSqlDialect.MYSQL, ArchiveSqlDialect.MARIADB -> " CHARACTER SET ascii COLLATE ascii_bin"
            else -> ""
        }
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.executeUpdate(
                    """
                    CREATE TABLE IF NOT EXISTS webhook_deliveries (
                        vendor varchar(63)$keyCollation NOT NULL,
                        delivery_id varchar(128)$keyCollation NOT NULL,
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
            dataSource.connection.use { connection ->
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
            val duplicate = when (dialect) {
                ArchiveSqlDialect.POSTGRESQL, ArchiveSqlDialect.H2 -> failure.sqlState == "23505"
                ArchiveSqlDialect.MYSQL, ArchiveSqlDialect.MARIADB -> failure.errorCode == 1062
                ArchiveSqlDialect.SQLITE -> failure.errorCode in setOf(19, 1555, 2067)
            }
            if (!duplicate || load(vendor, delivery.id) == null) throw failure
        }
    }

    override fun load(vendor: String, deliveryId: String): ByteArray? =
        dataSource.connection.use { connection -> readArchive(connection, vendor, deliveryId) }
}

internal fun readArchive(connection: Connection, vendor: String, deliveryId: String): ByteArray? =
    connection.prepareStatement(
        "SELECT body FROM webhook_deliveries WHERE vendor = ? AND delivery_id = ?"
    ).use { statement ->
        statement.setString(1, vendor)
        statement.setString(2, deliveryId)
        statement.executeQuery().use { if (it.next()) it.getBytes(1) else null }
    }
