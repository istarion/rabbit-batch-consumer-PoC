import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
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

public class RabbitBatchConsumer {
    private static final Logger logger = LoggerFactory.getLogger(RabbitBatchConsumer.class);
    private final int size;
    private final BlockingQueue<Delivery> deliveryList;
    private final Duration waitInterval;
    private volatile long lastInsert = System.nanoTime();
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final Channel channel;

    private final BatchDeliveryCallback callback = new BatchDeliveryCallback();

    private RabbitBatchConsumer(int size, Duration waitInterval, Channel channel) {
        this.size = size;
        this.deliveryList = new ArrayBlockingQueue<>(size);
        this.waitInterval = waitInterval;
        this.channel = channel;
    }

    public static List<Delivery> consumeBatch(
            Channel channel, String queueName, int maxCount, Duration timeout, Duration recheckInterval
    ) throws IOException, InterruptedException {
        RabbitBatchConsumer consumer = new RabbitBatchConsumer(maxCount, recheckInterval, channel);
        String consumerTag = channel.basicConsume(
                queueName, false,
                consumer.callback,
                (tag, sig) -> {
                    logger.info("Consumer [{}] shutdown", tag);
                }
        );
        return consumer.waitAndConsume(consumerTag, timeout);
    }

    private List<Delivery> waitAndConsume(String consumerTag, Duration maxInterval) throws InterruptedException, IOException {
        logger.info("Consumer tag in wait: {}", consumerTag);
        synchronized (isOpen) {
            Instant before = Instant.now();
            while (before.plus(maxInterval).isAfter(Instant.now())) {
                isOpen.wait(waitInterval.toMillis());
                if (lastInsert + waitInterval.toNanos() < System.nanoTime()) {
                    break;
                }
            }
            cancelSubscriptionIfNeeded(consumerTag);
            List<Delivery> result = new ArrayList<>(size);
            deliveryList.drainTo(result);
            return result;
        }
    }

    private void cancelSubscriptionIfNeeded(String consumerTag) throws IOException {
        if (isOpen.compareAndSet(true, false)) {
            logger.info("Cancelling {}", consumerTag);
            channel.basicCancel(consumerTag);
            logger.info("Cancelled {}", consumerTag);
        }
    }

    private class BatchDeliveryCallback implements DeliverCallback {
        @Override
        public void handle(String consumerTag, Delivery message) throws IOException {
            logger.info("Consumer tag: {}", consumerTag);
            synchronized (isOpen) {
                if (isOpen.get() && deliveryList.offer(message)) {
                    logger.info("queue size: {}", deliveryList.size());
                    lastInsert = System.nanoTime();
                    channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
                    logger.info("Acked message: {}", new String(message.getBody()));
                } else {
                    cancelSubscriptionIfNeeded(consumerTag);
                    isOpen.notifyAll();
                    channel.basicNack(message.getEnvelope().getDeliveryTag(), false, true);
                    logger.info("Nacked message: {}", new String(message.getBody()));
                }
            }
        }
    }
}
