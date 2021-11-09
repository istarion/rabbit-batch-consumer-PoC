import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class RabbitBatchConsumerMultiAck {
    private static final Logger logger = LoggerFactory.getLogger(RabbitBatchConsumerMultiAck.class);
    private final int size;
    private final BlockingQueue<byte[]> deliveryList;
    private final Duration waitInterval;
    private volatile long lastInsert = System.nanoTime();
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final Channel channel;
    private final int qos;

    private final RabbitConsumer rabbitConsumer = new RabbitConsumer();

    private RabbitBatchConsumerMultiAck(int size, Duration waitInterval, Channel channel, int qos) {
        this.qos = qos;
        this.size = size;
        this.deliveryList = new ArrayBlockingQueue<>(size);
        this.waitInterval = waitInterval;
        this.channel = channel;
    }

    public static List<byte[]> consumeBatch(
            Channel channel, String queueName, int maxCount, Duration timeout, Duration recheckInterval, int qos
    ) throws IOException, InterruptedException {
        RabbitBatchConsumerMultiAck consumer = new RabbitBatchConsumerMultiAck(maxCount, recheckInterval, channel, qos);
        String consumerTag = channel.basicConsume(queueName, false, consumer.rabbitConsumer);
        return consumer.waitAndConsume(consumerTag, timeout);
    }

    private List<byte[]> waitAndConsume(String consumerTag, Duration maxInterval) throws InterruptedException, IOException {
        synchronized (isOpen) {
            Instant before = Instant.now();
            while (before.plus(maxInterval).isAfter(Instant.now())) {
                isOpen.wait(waitInterval.toMillis());
                if (!isOpen.get() || lastInsert + waitInterval.toNanos() < System.nanoTime()) {
                    break;
                }
            }
            cancelSubscriptionIfNeeded(consumerTag);
            List<byte[]> result = new ArrayList<>(size);
            deliveryList.drainTo(result);
            return result;
        }
    }

    private void cancelSubscriptionIfNeeded(String consumerTag) throws IOException {
        if (isOpen.compareAndSet(true, false)) {
            logger.debug("Cancelling {}", consumerTag);
            channel.basicCancel(consumerTag);
            logger.debug("Cancelled {}", consumerTag);
            synchronized (isOpen) {
                isOpen.notifyAll();
            }
        }
    }

    private class RabbitConsumer extends DefaultConsumer {

        private RabbitConsumer() {
            super(channel);
        }

        @Override
        public void handleCancel(String consumerTag) throws IOException {
            logger.info("Cancel on {}", consumerTag);
            isOpen.set(false);
        }

        @Override
        public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
            logger.info("Shutdown signal on {}", consumerTag, sig);
        }

        @Override
        public void handleRecoverOk(String consumerTag) {
            logger.info("Recovered consumer {}", consumerTag);
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            logger.debug("Handling rabbit message, Consumer tag: {}, envelope: {}", consumerTag, envelope);
            if (isOpen.get() && deliveryList.offer(body)) {
                int queueSize = deliveryList.size();

                if (qos == 1 || queueSize % (qos / 2) == 0) {
                    channel.basicAck(envelope.getDeliveryTag(), true);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Acked message (multiple): {}", new String(body));
                    }
                }
                if (queueSize == size) {
                    logger.debug("queue size: {}", queueSize);
                    cancelSubscriptionIfNeeded(consumerTag);
                }
                lastInsert = System.nanoTime();
            } else {
                cancelSubscriptionIfNeeded(consumerTag);
                channel.basicNack(envelope.getDeliveryTag(), false, true);
                if (logger.isDebugEnabled()) {
                    logger.debug("Nacked message: {}", new String(body));
                }

            }
        }
    }
}
