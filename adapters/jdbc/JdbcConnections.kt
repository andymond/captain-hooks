package webhooks.jdbc

import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties

/** DriverManager wiring for the CLI and tests. Embedding applications can inject a DataSource. */
internal class JdbcConnections private constructor(
    val dialect: JdbcDialect,
    private val url: String,
    private val properties: Properties
) {
    fun open(): Connection = DriverManager.getConnection(url, properties)

    companion object {
        fun fromUrl(url: String?, user: String?, password: String?): JdbcConnections? {
            if (url == null || !url.startsWith("jdbc:")) return null
            val dialect = when (url.removePrefix("jdbc:").substringBefore(':', "")) {
                "postgresql" -> JdbcDialect.POSTGRESQL
                "mysql" -> JdbcDialect.MYSQL
                "mariadb" -> JdbcDialect.MARIADB
                "h2" -> JdbcDialect.H2
                "sqlite" -> JdbcDialect.SQLITE
                else -> return null
            }
            val properties = Properties().apply {
                if (dialect == JdbcDialect.POSTGRESQL) {
                    setProperty("connectTimeout", "5")
                    setProperty("socketTimeout", "10")
                }
                if (user != null) setProperty("user", user)
                if (password != null) setProperty("password", password)
            }
            return JdbcConnections(dialect, url, properties)
        }
    }
}
