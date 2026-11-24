package webhooks

import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties

internal fun postgresConnectionFactory(url: String, user: String?, password: String?): () -> Connection {
    val properties = Properties().apply {
        setProperty("connectTimeout", "5")
        setProperty("socketTimeout", "10")
        if (user != null) setProperty("user", user)
        if (password != null) setProperty("password", password)
    }
    return { DriverManager.getConnection(url, properties) }
}
