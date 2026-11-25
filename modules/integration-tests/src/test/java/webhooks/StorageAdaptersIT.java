package webhooks;

import com.mongodb.client.MongoClients;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.JedisPooled;
import webhooks.archive.JdbcWebhookArchive;
import webhooks.archive.MongoWebhookArchive;
import webhooks.archive.RedisWebhookArchive;
import webhooks.jdbc.JdbcDialect;
import webhooks.jobs.JdbcWebhookJobs;
import webhooks.jobs.MongoWebhookJobs;
import webhooks.jobs.RedisWebhookJobs;

import static org.junit.jupiter.api.Assertions.*;

class StorageAdaptersIT {
    @Test
    void postgresArchiveAndJobs() throws Exception {
        String serviceUrl = System.getenv("CAPTAIN_HOOKS_TEST_POSTGRES_URL");
        if (serviceUrl != null) {
            testPostgres(
                serviceUrl,
                System.getenv("CAPTAIN_HOOKS_TEST_POSTGRES_USER"),
                System.getenv("CAPTAIN_HOOKS_TEST_POSTGRES_PASSWORD")
            );
            return;
        }

        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")) {
            postgres.start();
            testPostgres(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        }
    }

    private void testPostgres(String url, String user, String password) throws Exception {
        var dataSource = new PGSimpleDataSource();
        dataSource.setUrl(url);
        dataSource.setUser(user);
        dataSource.setPassword(password);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute(schema("001_webhook_deliveries.sql"));
            statement.execute(schema("002_webhook_jobs.sql"));
        }
        var archive = new JdbcWebhookArchive(dataSource, JdbcDialect.POSTGRESQL);
        var jobs = new JdbcWebhookJobs(dataSource, JdbcDialect.POSTGRESQL);
        assertStorageContract(archive, jobs);
    }

    @Test
    void redisArchiveAndJobs() throws Exception {
        try (GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379)) {
            redis.start();
            try (var client = new JedisPooled(redis.getHost(), redis.getMappedPort(6379))) {
                assertStorageContract(new RedisWebhookArchive(client, "test:archive:"), new RedisWebhookJobs(client, "test:queue:"));
            }
        }
    }

    @Test
    void mongoArchiveAndJobs() throws Exception {
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:8.0"))) {
            mongo.start();
            try (var client = MongoClients.create(mongo.getReplicaSetUrl())) {
                var db = client.getDatabase("captain_hooks_test");
                var archive = new MongoWebhookArchive(db.getCollection("archive"));
                var jobs = new MongoWebhookJobs(db.getCollection("jobs"));
                jobs.createIndexes();
                assertStorageContract(archive, jobs);
            }
        }
    }

    private void assertStorageContract(ReadableWebhookArchive archive, WebhookJobs jobs) throws Exception {
        String vendor = "storage_" + UUID.randomUUID().toString().replace("-", "");
        byte[] rawBody = {0, -1, 7};
        byte[] projectedBody = {3, 5};
        archive.save(vendor, new Delivery("event", rawBody));
        archive.save(vendor, new Delivery("event", new byte[]{9}));
        assertArrayEquals(rawBody, archive.load(vendor, "event"));
        assertNull(archive.load(vendor + "_other", "event"));

        jobs.enqueue(new WebhookJob(vendor, "event", new JobInput.Projected(projectedBody)));
        jobs.enqueue(new WebhookJob(vendor, "event", new JobInput.Projected(new byte[]{9})));
        assertTrue(jobs.contains(vendor, "event"));
        assertFalse(jobs.contains(vendor + "_other", "event"));
        assertNull(jobs.claim(vendor, JobKind.ARCHIVE));

        var claim = claimFromCompetingWorkers(jobs, vendor);
        assertEquals(1, claim.getAttempt());
        assertArrayEquals(projectedBody, ((JobInput.Projected) claim.getJob().getInput()).getBody());
        jobs.fail(claim, false, 0, "temporary");
        var retry = assertNotNullClaim(jobs.claim(vendor, JobKind.PROJECTED));
        assertEquals(2, retry.getAttempt());
        assertNotEquals(claim.getToken(), retry.getToken());
        jobs.complete(claim);
        jobs.fail(retry, false, 0, "temporary");
        var third = assertNotNullClaim(jobs.claim(vendor, JobKind.PROJECTED));
        assertEquals(3, third.getAttempt());
        jobs.fail(third, true, 0, "terminal");
        assertNull(jobs.claim(vendor, JobKind.PROJECTED));

        jobs.enqueue(new WebhookJob(vendor, "event", JobInput.Archived.INSTANCE));
        assertNull(jobs.claim(vendor, JobKind.ARCHIVE));
        archive.save(vendor, new Delivery("reference", rawBody));
        jobs.enqueue(new WebhookJob(vendor, "reference", JobInput.Archived.INSTANCE));
        var reference = assertNotNullClaim(jobs.claim(vendor, JobKind.ARCHIVE));
        assertEquals(JobInput.Archived.INSTANCE, reference.getJob().getInput());
        assertArrayEquals(rawBody, archive.load(vendor, reference.getJob().getDeliveryId()));
        jobs.complete(reference);
        assertTrue(jobs.contains(vendor, "reference"));
        assertNull(jobs.claim(vendor, JobKind.ARCHIVE));
    }

    private ClaimedWebhook claimFromCompetingWorkers(WebhookJobs jobs, String vendor) throws Exception {
        var workers = Executors.newFixedThreadPool(2);
        var start = new CountDownLatch(1);
        try {
            var first = workers.submit(() -> {
                start.await();
                return jobs.claim(vendor, JobKind.PROJECTED);
            });
            var second = workers.submit(() -> {
                start.await();
                return jobs.claim(vendor, JobKind.PROJECTED);
            });
            start.countDown();
            var left = first.get(15, TimeUnit.SECONDS);
            var right = second.get(15, TimeUnit.SECONDS);
            assertTrue((left == null) != (right == null), "Exactly one worker must claim the job");
            return left == null ? right : left;
        } finally {
            workers.shutdownNow();
        }
    }

    private ClaimedWebhook assertNotNullClaim(ClaimedWebhook claim) {
        assertNotNull(claim);
        return claim;
    }

    private String schema(String name) throws IOException {
        try (var resource = getClass().getResourceAsStream("/META-INF/captain-hooks/db/" + name)) {
            assertNotNull(resource, "Missing packaged SQL script " + name);
            return new String(resource.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
