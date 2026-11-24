import webhooks.jdbc.JdbcConnections
import webhooks.jdbc.JdbcDialect

internal fun testJdbcConnectionSelection() {
    val examples = mapOf(
        "jdbc:postgresql://localhost/hooks" to JdbcDialect.POSTGRESQL,
        "jdbc:mysql://localhost/hooks" to JdbcDialect.MYSQL,
        "jdbc:mariadb://localhost/hooks" to JdbcDialect.MARIADB,
        "jdbc:h2:mem:hooks" to JdbcDialect.H2,
        "jdbc:sqlite:hooks.db" to JdbcDialect.SQLITE
    )
    for ((url, expected) in examples) check(JdbcConnections.fromUrl(url, null, null)?.dialect == expected)
    for (url in listOf(null, "postgresql://localhost/hooks", "jdbc:postgresql", "jdbc:other:hooks")) {
        check(JdbcConnections.fromUrl(url, null, null) == null)
    }
    println("PASS JDBC URL dialect selection without opening connections")
}
