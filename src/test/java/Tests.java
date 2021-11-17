import com.rabbitmq.client.*;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Testcontainers
public class Tests {
    private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";
    private static final Logger logger = LoggerFactory.getLogger(Tests.class);

    public Network network = Network.newNetwork();

    @Container
    public ToxiproxyContainer toxiproxyContainer = new ToxiproxyContainer("shopify/toxiproxy")
            .withNetwork(network)
            .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);

    @Container
    public RabbitMQContainer rabbitMQContainer = new RabbitMQContainer("rabbitmq").withNetwork(network);

    public Connection rabbitConnection;

    @BeforeEach
    public void setUp() throws IOException, TimeoutException {
        ToxiproxyContainer.ContainerProxy toxiRabbit = toxiproxyContainer.getProxy(rabbitMQContainer, 5672);
        toxiRabbit.toxics()
                .latency("latency-up", ToxicDirection.DOWNSTREAM, 2)
                .setJitter(5);
        toxiRabbit.toxics()
                .latency("latency-down", ToxicDirection.DOWNSTREAM, 2)
                .setJitter(5);
        toxiRabbit.toxics()
                .bandwidth("bandwidth-up", ToxicDirection.UPSTREAM, 4096);  // 1 MB/s
        toxiRabbit.toxics()
                .bandwidth("bandwidth-down", ToxicDirection.DOWNSTREAM, 4096);  // 1 MB/s
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(toxiRabbit.getContainerIpAddress());
        factory.setPort(toxiRabbit.getProxyPort());
        rabbitConnection = factory.newConnection();
    }

    @AfterEach
    public void tearDown() throws IOException {
        rabbitConnection.close();
    }

    @Test
    public void basicTest() throws IOException, TimeoutException {
        String exchangeName = "basicTest";
        String routingKey = "test";
        try (Channel channel = rabbitConnection.createChannel()) {
            channel.exchangeDeclare(exchangeName, "direct", true);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, exchangeName, routingKey);

            byte[] messageBodyBytes = "Hello, world!".getBytes();
            channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);

            GetResponse resp = channel.basicGet(queueName, true);
            logger.info("Resp: {}", resp);
            logger.info("Body: {}", new String(resp.getBody()));
        }
    }

    @Test
    public void testBatch() throws IOException, TimeoutException {
        batchReadWithBasicGet(100);
    }

    private double batchReadWithBasicGet(int count) throws IOException, TimeoutException {
        String exchangeName = "testBatch";
        String routingKey = "test";
        String queueName = getTempQueue(exchangeName, routingKey);
        try (Channel channel = rabbitConnection.createChannel()) {
            fillQueue(channel, exchangeName, routingKey, 1000);
            long before = System.nanoTime();
            for (int i = 0; i < count; ++i) {
                GetResponse resp = channel.basicGet(queueName, false);
                logger.info("Resp: {}", resp);
                logger.info("Body: {}", new String(resp.getBody()));
                channel.basicAck(resp.getEnvelope().getDeliveryTag(), false);
            }
            double processingTime = (System.nanoTime() - before) / 1_000_000.0;
            logger.info("Processing time: {}ms", processingTime);
            return processingTime;
        }
    }

    private String getTempQueue(String exchangeName, String routingKey) throws IOException, TimeoutException {
        try (Channel channel = rabbitConnection.createChannel()) {
            channel.exchangeDeclare(exchangeName, "direct", true);
            String queueName = channel.queueDeclare(
                    "", true, false, false, null
            ).getQueue();
            channel.queueBind(queueName, exchangeName, routingKey);
            return queueName;
        }
    }

    private void fillQueue(Channel channel, String exchangeName, String routingKey, int count) throws IOException {
        channel.confirmSelect();
        for (int i = 0; i < count; i++) {
            byte[] messageBodyBytes = ("Hello, world! ".repeat(50) + i).getBytes();
            channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);
        }
        try {
            channel.waitForConfirmsOrDie(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
//        channel.waitForConfirms()
        logger.info("Queue filled");
    }

    @Test
    public void testConsumeBatch() throws IOException, TimeoutException {
        String exchangeName = "testConsumeBatch";
        String routingKey = "test";
        String queueName = getTempQueue(exchangeName, routingKey);
        try (Channel channel = rabbitConnection.createChannel()) {
            fillQueue(channel, exchangeName, routingKey, 1000);

            long before = System.nanoTime();
            channel.basicQos(1);
            List<Delivery> messages = getDeliveries(queueName, channel, 50);
            logger.info("Resp: {}", messages);
            List<Delivery> messages2 = getDeliveries(queueName, channel, 50);
            logger.info("Resp2: {}", messages2);
            logger.info("Processing time: {}ms", (System.nanoTime() - before) / 1_000_000.0);
        }
    }

    @Test
    public void testRabbitBatchConsumerWithoutQos() throws IOException, TimeoutException, InterruptedException {
        testRabbitBatchConsumer(1, 100);
    }

    @Test
    public void testRabbitBatchConsumerWithoutQosSingle() throws IOException, TimeoutException, InterruptedException {
        testRabbitBatchConsumer(1, 1);
    }

    @Test
    public void testRabbitBatchConsumerQos() throws IOException, TimeoutException, InterruptedException {
        testRabbitBatchConsumer(12, 100);
    }

    private double testRabbitBatchConsumer(int qos, int batchSize) throws IOException, TimeoutException, InterruptedException {
        String exchangeName = "testConsumeBatchWithClass";
        String routingKey = "test";
        String queueName = getTempQueue(exchangeName, routingKey);
        try (Channel channel = rabbitConnection.createChannel()) {
            fillQueue(channel, exchangeName, routingKey, 10000);

            long before = System.nanoTime();
            channel.basicQos(qos, false);
            List<byte[]> messages = RabbitBatchConsumer.consumeBatch(
                    channel, queueName, batchSize, Duration.ofSeconds(3), Duration.ofMillis(50)
            );

            logger.info("Resp: {}", messages);
            double processingTimeMillis = (System.nanoTime() - before) / 1_000_000.0;
            logger.info("Processing time: {}ms", processingTimeMillis);
            Thread.sleep(1000);
            return processingTimeMillis;
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            logger.info("Exception!", e);
            return 0;
        }
    }

    //    100_000   =  64547.692838ms
//    1_000_000 = 280339.738331ms
    @Disabled("Long")
    @Test
    public void testMillionGet() throws IOException, TimeoutException, InterruptedException {
        final int TEST_SIZE = 1000000;
        String exchangeName = "testMillionGet";
        String routingKey = "test";
        String queueName = getTempQueue(exchangeName, routingKey);
        int qos = 20;
        int batchSize = 500;
        AtomicInteger receivedCount = new AtomicInteger(0);
        try (Channel channel = rabbitConnection.createChannel()) {
            fillQueue(channel, exchangeName, routingKey, TEST_SIZE);

            long before = System.nanoTime();
            channel.basicQos(qos, false);
            ExecutorService executorService = Executors.newFixedThreadPool(4);
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    List<byte[]> messages;
                    do {
                        try {
                            messages = RabbitBatchConsumer.consumeBatch(
                                    channel, queueName, batchSize, Duration.ofSeconds(3), Duration.ofMillis(50)
                            );
                        } catch (IOException | InterruptedException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                        logger.info("Million test {}, got {}", receivedCount.get(), messages.size());
                    } while (receivedCount.addAndGet(messages.size()) < TEST_SIZE);
                }
            };
            Future<?> f1 = executorService.submit(task);
            Future<?> f2 = executorService.submit(task);
            Future<?> f3 = executorService.submit(task);
            Future<?> f4 = executorService.submit(task);

            f1.get();
            f2.get();
            f3.get();
            f4.get();

            double processingTimeMillis = (System.nanoTime() - before) / 1_000_000.0;
            logger.info("Processing time: {}ms", processingTimeMillis);
            Thread.sleep(1000);
//            return processingTimeMillis;
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            logger.info("Exception!", e);
        }
    }

    @Test
    public void lotOfSubscriptions() throws IOException, TimeoutException, InterruptedException {
        final int SUB_SIZE = 10_000;
        String exchangeName = "lotOfSubscriptions";
        String routingKey = "test";
        String queueName = getTempQueue(exchangeName, routingKey);
        int qos = 20;
        int batchSize = 10;
        AtomicInteger receivedCount = new AtomicInteger(0);
        try (Channel channel = rabbitConnection.createChannel()) {
//                fillQueue(channel, exchangeName, routingKey, TEST_SIZE);

            long before = System.nanoTime();
            channel.basicQos(qos, false);
            for (int i = 0; i < SUB_SIZE; ++i) {
                channel.basicConsume(
                        queueName, true, (consumerTag, message) -> receivedCount.incrementAndGet(), (consumerTag, sig) -> {
                        });
            }

            fillQueue(channel, exchangeName, routingKey, 1000);
            while (receivedCount.get() < 1000) {
                Thread.sleep(1000);
                logger.info("Consumed: {}", receivedCount.get());
            }
            Thread.sleep(1000);

        }
    }

    //    100_000   =  13050.919505ms
//    1_000_000 = 124124.508242ms
//    @Disabled("Long")
    @Test
    public void testPlainSubscribe() throws IOException, TimeoutException, InterruptedException {
        final int TEST_SIZE = 1_000_000;
        String exchangeName = "testPlainSubscribe";
        String routingKey = "test";
        String queueName = getTempQueue(exchangeName, routingKey);
        int qos = 20;
        int batchSize = 100;
        AtomicInteger receivedCount = new AtomicInteger(0);
        try (Channel channel = rabbitConnection.createChannel()) {
            fillQueue(channel, exchangeName, routingKey, TEST_SIZE);

            long before = System.nanoTime();
            channel.basicQos(qos, false);
            channel.basicConsume(
                    queueName, true, (consumerTag, message) -> receivedCount.incrementAndGet(), (consumerTag, sig) -> {
                    });
            do {
                Thread.sleep(1000);
                logger.info("Million test, got {}", receivedCount.get());
            } while (receivedCount.get() < TEST_SIZE);

            double processingTimeMillis = (System.nanoTime() - before) / 1_000_000.0;
            logger.info("Processing time: {}ms", processingTimeMillis);
            Thread.sleep(1000);
//            return processingTimeMillis;
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            logger.info("Exception!", e);
        }
    }

    @Test
    public void testLeak() throws IOException, TimeoutException, InterruptedException {
        final int TEST_SIZE = 100_000;
        String exchangeName = "testLeak";
        String routingKey = "test";
        String queueName = getTempQueue(exchangeName, routingKey);
        int qos = 20;
        int batchSize = 100;
        AtomicInteger receivedCount = new AtomicInteger(0);
        try (Channel channel = rabbitConnection.createChannel()) {
            fillQueue(channel, exchangeName, routingKey, TEST_SIZE);

            long before = System.nanoTime();
            channel.basicQos(qos, false);
            ExecutorService executorService = Executors.newFixedThreadPool(16);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 42; ++i) {
                futures.add(executorService.submit((Runnable) () -> {
                            while (receivedCount.get() < TEST_SIZE) {
                                List<byte[]> bytes = null;
                                try {
                                    bytes = RabbitBatchConsumer.consumeBatch(
                                            channel, queueName, batchSize, Duration.ofSeconds(3), Duration.ofMillis(100)
                                    );
                                    receivedCount.addAndGet(bytes.size());
                                } catch (IOException | InterruptedException e) {
                                    logger.error("ex", e);
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                ));
            }
            do {
                Thread.sleep(1000);
                logger.info("Leak test, got {}", receivedCount.get());
            } while (receivedCount.get() < TEST_SIZE);

            double processingTimeMillis = (System.nanoTime() - before) / 1_000_000.0;
            logger.info("Processing time: {}ms", processingTimeMillis);
//            return processingTimeMillis;
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            logger.info("Exception!", e);
        }
    }

    private double testRabbitBatchConsumerMultipleAck(int qos, int batchSize) throws
            IOException, TimeoutException, InterruptedException {
        String exchangeName = "testConsumeBatchWithClass";
        String routingKey = "test";
        String queueName = getTempQueue(exchangeName, routingKey);
        try (Channel channel = rabbitConnection.createChannel()) {
            fillQueue(channel, exchangeName, routingKey, 10000);

            long before = System.nanoTime();
            channel.basicQos(qos, false);
            List<byte[]> messages = RabbitBatchConsumerMultiAck.consumeBatch(
                    channel, queueName, batchSize, Duration.ofSeconds(3), Duration.ofMillis(50), qos
            );

            logger.info("Resp: {}", messages);
            double processingTimeMillis = (System.nanoTime() - before) / 1_000_000.0;
            logger.info("Processing time: {}ms", processingTimeMillis);
            Thread.sleep(1000);
            return processingTimeMillis;
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            logger.info("Exception!", e);
            return 0;
        }
    }

    @Test
    public void testConsumeBatchWithClassParallel() throws IOException, TimeoutException {
        String exchangeName = "testConsumeBatchWithClassParallel";
        String routingKey = "test";
        String queueName = getTempQueue(exchangeName, routingKey);
        try (Channel channel = rabbitConnection.createChannel()) {
            fillQueue(channel, exchangeName, routingKey, 400);
            Thread.sleep(10);
            Assertions.assertEquals(400, channel.messageCount(queueName));

            long before = System.nanoTime();
            channel.basicQos(12);

            ExecutorService executorService = Executors.newCachedThreadPool();
            List<Future<List<byte[]>>> futures = new ArrayList<>();
            for (int i = 0; i < 3; ++i) {
                futures.add(executorService.submit(() ->
                        RabbitBatchConsumer.consumeBatch(
                                channel, queueName, 100, Duration.ofSeconds(3), Duration.ofMillis(100)
                        ))
                );
            }

            for (int i = 0; i < futures.size(); ++i) {
                List<byte[]> deliveries = futures.get(i).get();
                logger.info(
                        "Resp {}: {}", i,
                        deliveries.stream().map(delivery -> new String(delivery, StandardCharsets.UTF_8))
                                .collect(Collectors.toList())
                );
                Assertions.assertEquals(100, deliveries.size());
            }

            logger.info("Processing time: {}ms", (System.nanoTime() - before) / 1_000_000.0);

            Thread.sleep(10);
            Assertions.assertEquals(100, channel.messageCount(queueName));
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConsumeBatchWithClassParallelMultiack() throws IOException, TimeoutException {
        String exchangeName = "testConsumeBatchWithClassParallel";
        String routingKey = "test";
        String queueName = getTempQueue(exchangeName, routingKey);
        try (Channel channel = rabbitConnection.createChannel()) {
            fillQueue(channel, exchangeName, routingKey, 400);
            Thread.sleep(10);
            Assertions.assertEquals(400, channel.messageCount(queueName));

            long before = System.nanoTime();
            channel.basicQos(12);

            ExecutorService executorService = Executors.newCachedThreadPool();
            List<Future<List<byte[]>>> futures = new ArrayList<>();
            for (int i = 0; i < 3; ++i) {
                futures.add(executorService.submit(() ->
                        RabbitBatchConsumerMultiAck.consumeBatch(
                                channel, queueName, 100, Duration.ofSeconds(3), Duration.ofMillis(100), 12
                        ))
                );
            }

            for (int i = 0; i < futures.size(); ++i) {
                List<byte[]> deliveries = futures.get(i).get();
                logger.info(
                        "Resp {}: {}", i,
                        deliveries.stream().map(delivery -> new String(delivery, StandardCharsets.UTF_8))
                                .collect(Collectors.toList())
                );
                Assertions.assertEquals(100, deliveries.size());
            }

            logger.info("Processing time: {}ms", (System.nanoTime() - before) / 1_000_000.0);

            Thread.sleep(10);
            Assertions.assertEquals(100, channel.messageCount(queueName));
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void showStats() throws IOException, TimeoutException, InterruptedException {
        List<StatRun> statRuns = new ArrayList<>();
        for (int batchSize : new int[]{1, 10, 30, 100, 500}) {
            statRuns.add(
                    new StatRun(batchSize, 1, "basicGet", batchReadWithBasicGet(batchSize))
            );
            for (int qos : new int[]{1, 5, 12, 50}) {
                statRuns.add(
                        new StatRun(batchSize, qos, "RabbitBatchConsumer", testRabbitBatchConsumer(qos, batchSize))
                );
//                statRuns.add(
//                        new StatRun(batchSize, qos, "RabbitBatchConsumerMultiAck", testRabbitBatchConsumerMultipleAck(qos, batchSize))
//                );
            }
        }

        System.out.println("RESULTS:");
        System.out.println("== ToxyProxy latency 10ms =====================================");
        for (StatRun statRun : statRuns) {
            System.out.printf("| %-30s | batch=%-3d | qos=%-3d | %-12fms |\n",
                    statRun.getType(), statRun.getBatchSize(), statRun.getQos(), statRun.getValue()
            );
        }
        System.out.println("===============================================================");
    }

    @NotNull
    private List<Delivery> getDeliveries(String queueName, Channel channel, int count) throws IOException {
        List<Delivery> messages = new ArrayList<Delivery>(count);
        channel.basicConsume(
                queueName, false,
                (consumerTag, message) -> {
                    messages.add(message);
                    if (messages.size() == count) {
                        channel.basicAck(message.getEnvelope().getDeliveryTag(), true);
                        channel.basicCancel(consumerTag);
                    }
                }, tag -> {
                    logger.info("Cancel {}", tag);
                }
        );
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return messages;
    }
}
