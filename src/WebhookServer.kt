package webhooks

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.nio.file.Path
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

internal class RunningWebhookServer(
    private val server: HttpServer,
    private val executor: ExecutorService,
    private val worker: AutoCloseable?
) : AutoCloseable {
    val port: Int get() = server.address.port

    override fun close() {
        server.stop(1)
        executor.shutdownNow()
        worker?.close()
    }
}

internal fun startWebhookServer(
    address: InetSocketAddress,
    handler: WebhookHandler,
    secret: ByteArray,
    dataDirectory: Path,
    archive: WebhookArchive? = null,
    jobs: WebhookJobs? = null
): RunningWebhookServer {
    val endpoint = WebhookEndpoint(handler, secret, dataDirectory, archive, jobs)
    val server = HttpServer.create(address, 0)
    val executor = Executors.newFixedThreadPool(8)
    server.executor = executor
    server.createContext(endpoint.route) { exchange ->
        exchange.use {
            val response = if (exchange.requestURI.rawPath == endpoint.route) {
                endpoint.handle(
                    exchange.requestMethod,
                    exchange.requestHeaders,
                    exchange.requestBody
                )
            } else WebhookResponse(404)
            response.headers.forEach { (name, value) -> exchange.responseHeaders.set(name, value) }
            exchange.sendResponseHeaders(response.status, -1)
        }
    }
    server.start()
    return RunningWebhookServer(server, executor, endpoint.startWorker())
}
