package webhooks.jdbc

import java.sql.SQLException

/** SQL storage formats supported by the bundled schema initializer. */
enum class JdbcDialect(internal val bodyType: String, internal val timeType: String) {
    POSTGRESQL("bytea", "timestamptz"),
    MYSQL("longblob", "timestamp"),
    MARIADB("longblob", "timestamp"),
    H2("blob", "timestamp with time zone"),
    SQLITE("blob", "timestamp")
}

internal fun JdbcDialect.isDuplicate(failure: SQLException): Boolean = when (this) {
    JdbcDialect.POSTGRESQL, JdbcDialect.H2 -> failure.sqlState == "23505"
    JdbcDialect.MYSQL, JdbcDialect.MARIADB -> failure.errorCode == 1062
    JdbcDialect.SQLITE -> failure.errorCode in setOf(19, 1555, 2067)
}


internal val JdbcDialect.keyCollation: String
    get() = when (this) {
        JdbcDialect.MYSQL, JdbcDialect.MARIADB -> " CHARACTER SET ascii COLLATE ascii_bin"
        else -> ""
    }
