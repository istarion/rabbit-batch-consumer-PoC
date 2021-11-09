import com.rabbitmq.client.*;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
    private ToxiproxyContainer.ContainerProxy toxiRabbit;

    public Connection rabbitConnection;

    @BeforeEach
    public void setUp() throws IOException, TimeoutException {
        toxiRabbit = toxiproxyContainer.getProxy(rabbitMQContainer, 5672);
        toxiRabbit.toxics()
                .latency("latency-up", ToxicDirection.DOWNSTREAM, 10)
                .setJitter(7);
        toxiRabbit.toxics()
                .latency("latency-down", ToxicDirection.DOWNSTREAM, 10)
                .setJitter(7);
        toxiRabbit.toxics()
                .bandwidth("bandwidth-up", ToxicDirection.UPSTREAM, 1024);  // 1 MB/s
        toxiRabbit.toxics()
                .bandwidth("bandwidth-down", ToxicDirection.DOWNSTREAM, 1024);  // 1 MB/s
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
                GetResponse resp = channel.basicGet(queueName, true);
                logger.info("Resp: {}", resp);
                logger.info("Body: {}", new String(resp.getBody()));
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
            byte[] messageBodyBytes = ("Hello, world! " + i).getBytes();
            channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);
        }
        try {
            channel.waitForConfirmsOrDie(10000);
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
            List<Delivery> messages = RabbitBatchConsumer.consumeBatch(
                    channel, queueName, batchSize, Duration.ofSeconds(3), Duration.ofMillis(50)
            );

            logger.info("Resp: {}", messages);
            double processingTimeMillis = (System.nanoTime() - before) / 1_000_000.0;
            logger.info("Processing time: {}ms", processingTimeMillis);
            return processingTimeMillis;
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw e;
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
            List<Future<List<Delivery>>> futures = new ArrayList<>();
            for (int i = 0; i < 3; ++i) {
                futures.add(executorService.submit(() ->
                        RabbitBatchConsumer.consumeBatch(
                                channel, queueName, 100, Duration.ofSeconds(3), Duration.ofMillis(100)
                        ))
                );
            }

            for (int i = 0; i < futures.size(); ++i) {
                List<Delivery> deliveries = futures.get(i).get();
                logger.info(
                        "Resp {}: {}", i,
                        deliveries.stream().map(delivery -> new String(delivery.getBody(), StandardCharsets.UTF_8))
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
        double basicGet1 = batchReadWithBasicGet(1);
        double rbcQ11 = testRabbitBatchConsumer(1, 1);
        double rbcQ51 = testRabbitBatchConsumer(5, 1);
        double rbcQ121 = testRabbitBatchConsumer(12, 1);

        double basicGet30 = batchReadWithBasicGet(30);
        double rbcQ130 = testRabbitBatchConsumer(1, 30);
        double rbcQ530 = testRabbitBatchConsumer(5, 30);
        double rbcQ1230 = testRabbitBatchConsumer(12, 30);

        double basicGet100 = batchReadWithBasicGet(100);
        double rbcQ1100 = testRabbitBatchConsumer(1, 100);
        double rbcQ5100 = testRabbitBatchConsumer(5, 100);
        double rbcQ12100 = testRabbitBatchConsumer(12, 100);

        logger.info("RESULTS:");
        logger.info("== ToxyProxy latency 10ms ======================================");
        logger.info("BasicGet batch(1):\t\t\t\t\t\t {}ms", basicGet1);
        logger.info("RabbitBatchConsumer batch(1) qos=1:\t\t {}ms", rbcQ11);
        logger.info("RabbitBatchConsumer batch(1) qos=5:\t\t {}ms", rbcQ51);
        logger.info("RabbitBatchConsumer batch(1) qos=12:\t\t {}ms\n", rbcQ121);
        logger.info("BasicGet batch(30):\t\t\t\t\t\t {}ms", basicGet30);
        logger.info("RabbitBatchConsumer batch(30) qos=1:\t\t {}ms", rbcQ130);
        logger.info("RabbitBatchConsumer batch(30) qos=5:\t\t {}ms", rbcQ530);
        logger.info("RabbitBatchConsumer batch(30) qos=12:\t {}ms\n", rbcQ1230);
        logger.info("BasicGet batch(100):\t\t\t\t\t\t {}ms", basicGet100);
        logger.info("RabbitBatchConsumer batch(100) qos=1:\t {}ms", rbcQ1100);
        logger.info("RabbitBatchConsumer batch(100) qos=5:\t {}ms", rbcQ5100);
        logger.info("RabbitBatchConsumer batch(100) qos=12:\t {}ms", rbcQ12100);
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
