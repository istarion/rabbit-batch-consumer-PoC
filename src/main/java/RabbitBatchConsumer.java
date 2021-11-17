import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RabbitBatchConsumer {
    private static final Logger logger = LoggerFactory.getLogger(RabbitBatchConsumer.class);
    private final int size;
    private final BlockingQueue<byte[]> deliveryList;
    private final Duration waitInterval;
    private volatile long lastInsert = System.nanoTime();
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final Channel channel;
    private final RabbitConsumer rabbitConsumer = new RabbitConsumer();

    private RabbitBatchConsumer(int size, Duration waitInterval, Channel channel) {
        this.size = size;
        this.deliveryList = new ArrayBlockingQueue<>(size);
        this.waitInterval = waitInterval;
        this.channel = channel;
    }

    public static List<byte[]> consumeBatch(
            Channel channel, String queueName, int maxCount, Duration timeout, Duration recheckInterval
    ) throws IOException, InterruptedException {
        RabbitBatchConsumer consumer = new RabbitBatchConsumer(maxCount, recheckInterval, channel);
        String consumerTag;
        synchronized (channel) {
            consumerTag = channel.basicConsume(queueName, false, consumer.rabbitConsumer);
        }

        logger.debug("Subscribed to: {}", consumerTag);
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
            synchronized (channel) {
                channel.basicCancel(consumerTag);
            }
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
                if (queueSize == size) {
                    logger.debug("queue size: {}", queueSize);
                    cancelSubscriptionIfNeeded(consumerTag);
                }
                lastInsert = System.nanoTime();
                channel.basicAck(envelope.getDeliveryTag(), false);
                if (logger.isDebugEnabled()) {
                    logger.debug("Acked message: {}", new String(body));
                }
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
