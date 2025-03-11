package upic.server.messaging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import upic.server.config.RabbitMQConfig;
import upic.server.config.ServerConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Publisher for sending messages to RabbitMQ.
 * Supports both individual and batch message publishing.
 */
public class RabbitMQPublisher {
    private static final Logger LOGGER = Logger.getLogger(RabbitMQPublisher.class.getName());

    private final RabbitMQChannelPool channelPool;
    private final String queueName;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutor;

    // Message batching
    private final ConcurrentLinkedQueue<BatchItem> messageBatch = new ConcurrentLinkedQueue<>();
    private static final int BATCH_SIZE = ServerConfig.BATCH_SIZE;
    private static final long BATCH_FLUSH_INTERVAL_MS = ServerConfig.BATCH_FLUSH_INTERVAL_MS;

    // Performance metrics
    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder publishErrors = new LongAdder();

    /**
     * Creates a new RabbitMQ publisher.
     * @param channelPoolSize The size of the channel pool
     * @throws Exception If initialization fails
     */
    public RabbitMQPublisher(int channelPoolSize) throws Exception {
        this.queueName = RabbitMQConfig.getQueueName();
        this.channelPool = new RabbitMQChannelPool(channelPoolSize);

        // Create thread pools for async operations
        this.executorService = Executors.newFixedThreadPool(
                ServerConfig.getOptimalThreadCount() / 4 // Use a fraction of server threads
        );
        this.scheduledExecutor = Executors.newScheduledThreadPool(2);

        // Schedule batch message processor
        scheduledExecutor.scheduleAtFixedRate(
                this::flushMessageBatch,
                BATCH_FLUSH_INTERVAL_MS,
                BATCH_FLUSH_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );

        LOGGER.info("RabbitMQ publisher initialized with queue: " + queueName +
                ", channel pool size: " + channelPoolSize);
    }

    /**
     * Publishes a message to the queue using the batching mechanism.
     * @param messageId A unique ID for the message
     * @param messageBody The message body as a string
     * @return true if the message was successfully added to the batch, false otherwise
     */
    public boolean publishMessage(String messageId, String messageBody) {
        try {
            // Add message to batch
            BatchItem batchItem = new BatchItem(messageId, messageBody);
            boolean added = messageBatch.add(batchItem);

            // If batch size reached, trigger flush in separate thread
            if (messageBatch.size() >= BATCH_SIZE) {
                triggerBatchFlush();
            }

            return added; // Return whether the message was successfully added to the batch
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error adding message to batch", e);
            publishErrors.increment();
            return false;
        }
    }

    /**
     * Publishes a message to the queue immediately (bypassing the batch).
     * @param messageBody The message body as a string
     * @return True if successful, false otherwise
     */
    public boolean publishMessageImmediate(String messageBody) {
        Channel channel = null;
        try {
            channel = channelPool.getChannel();
            // Enable publisher confirms for this channel
            channel.confirmSelect();
            channel.basicPublish(
                    "", // Default exchange
                    queueName,
                    MessageProperties.PERSISTENT_TEXT_PLAIN, // Make message persistent
                    messageBody.getBytes()
            );
            // Wait for confirmation (with timeout)
            boolean confirmed = channel.waitForConfirms(1000);
            if (confirmed) {
                messagesSent.increment();
                return true;
            } else {
                publishErrors.increment();
                return false;
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error publishing immediate message", e);
            publishErrors.increment();
            return false;
        } finally {
            if (channel != null) {
                channelPool.returnChannel(channel);
            }
        }
    }

    /**
     * Triggers an immediate batch flush in a separate thread.
     */
    private void triggerBatchFlush() {
        executorService.submit(this::flushMessageBatch);
    }

    /**
     * Flushes the current batch of messages to RabbitMQ.
     */
    private synchronized void flushMessageBatch() {
        if (messageBatch.isEmpty()) {
            return;
        }

        List<BatchItem> batchToSend = new ArrayList<>(BATCH_SIZE);
        int count = 0;

        // Take up to BATCH_SIZE messages
        while (!messageBatch.isEmpty() && count < BATCH_SIZE) {
            BatchItem item = messageBatch.poll();
            if (item != null) {
                batchToSend.add(item);
                count++;
            }
        }

        if (!batchToSend.isEmpty()) {
            Channel channel = null;
            try {
                channel = channelPool.getChannel();

                // Use transaction for batch reliability
                channel.txSelect();

                for (BatchItem item : batchToSend) {
                    channel.basicPublish(
                            "", // Default exchange
                            queueName,
                            MessageProperties.PERSISTENT_TEXT_PLAIN, // Make message persistent
                            item.messageBody.getBytes()
                    );
                }

                // Commit the transaction
                channel.txCommit();

                // Update metrics
                messagesSent.add(batchToSend.size());

                if (Math.random() < 0.01) { // Log 1% of batch sends
                    LOGGER.info("Sent batch of " + batchToSend.size() + " messages to RabbitMQ");
                }
            } catch (Exception e) {
                // Log error and try to rollback transaction
                LOGGER.log(Level.WARNING, "Error sending batch to RabbitMQ", e);
                publishErrors.increment();

                if (channel != null && channel.isOpen()) {
                    try {
                        channel.txRollback();

                        // Put messages back to the batch queue for retry
                        messageBatch.addAll(batchToSend);
                    } catch (IOException rollbackEx) {
                        LOGGER.log(Level.SEVERE, "Failed to rollback transaction", rollbackEx);
                    }
                }
            } finally {
                if (channel != null) {
                    channelPool.returnChannel(channel);
                }
            }
        }
    }

    /**
     * Get the total number of messages sent.
     * @return The count of messages sent
     */
    public long getMessagesSent() {
        return messagesSent.sum();
    }

    /**
     * Get the number of publish errors encountered.
     * @return The count of publish errors
     */
    public long getPublishErrors() {
        return publishErrors.sum();
    }

    /**
     * Shutdown the publisher, flushing any pending messages.
     */
    public void shutdown() {
        // Final flush of any messages
        flushMessageBatch();

        // Shutdown thread pools
        scheduledExecutor.shutdown();
        executorService.shutdown();

        try {
            if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutor.shutdownNow();
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Close the channel pool
        channelPool.close();

        LOGGER.info("RabbitMQ publisher shutdown complete, sent " + messagesSent.sum() + " messages total");
    }

    /**
     * Class to hold batch message items
     */
    private static class BatchItem {
        final String id;
        final String messageBody;

        BatchItem(String id, String messageBody) {
            this.id = id;
            this.messageBody = messageBody;
        }
    }
}