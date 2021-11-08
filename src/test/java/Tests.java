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

    public Connection rabbitConnection;

    @BeforeEach
    public void setUp() throws IOException, TimeoutException {
        ToxiproxyContainer.ContainerProxy toxiRabbit = toxiproxyContainer.getProxy(rabbitMQContainer, 5672);
        toxiRabbit.toxics()
                .latency("latency-up", ToxicDirection.DOWNSTREAM, 20)
                .setJitter(10);
        toxiRabbit.toxics()
                .latency("latency-down", ToxicDirection.DOWNSTREAM, 20)
                .setJitter(10);
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
        String exchangeName = "testBatch";
        String routingKey = "test";
        String queueName = getTempQueue(exchangeName, routingKey);
        try (Channel channel = rabbitConnection.createChannel()) {
            fillQueue(channel, exchangeName, routingKey, 10000);
            long before = System.nanoTime();
            for (int i = 0; i < 100; ++i) {
                GetResponse resp = channel.basicGet(queueName, true);
                logger.info("Resp: {}", resp);
                logger.info("Body: {}", new String(resp.getBody()));
            }
            logger.info("Processing time: {}ms", (System.nanoTime() - before) / 1_000_000.0);
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
        for (int i = 0; i < count; i++) {
            byte[] messageBodyBytes = ("Hello, world! " + i).getBytes();
            channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);
        }
    }

    @Test
    public void testConsumeBatch() throws IOException, TimeoutException {
        String exchangeName = "testConsumeBatch";
        String routingKey = "test";
        String queueName = getTempQueue(exchangeName, routingKey);
        try (Channel channel = rabbitConnection.createChannel()) {
            fillQueue(channel, exchangeName, routingKey, 10000);

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
    public void testConsumeBatchWithClass() throws IOException, TimeoutException {
        String exchangeName = "testConsumeBatchWithClass";
        String routingKey = "test";
        String queueName = getTempQueue(exchangeName, routingKey);
        try (Channel channel = rabbitConnection.createChannel()) {
            fillQueue(channel, exchangeName, routingKey, 10000);

            long before = System.nanoTime();
            channel.basicQos(10);
            List<Delivery> messages = RabbitBatchConsumer.consumeBatch(
                    channel, queueName, 100, Duration.ofSeconds(3), Duration.ofMillis(100)
            );

            logger.info("Resp: {}", messages);
            logger.info("Processing time: {}ms", (System.nanoTime() - before) / 1_000_000.0);
        } catch (InterruptedException e) {
            e.printStackTrace();
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
