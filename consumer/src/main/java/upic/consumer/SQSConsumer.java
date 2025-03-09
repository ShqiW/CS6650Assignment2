package upic.consumer;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.google.gson.Gson;

import upic.consumer.config.CircuitBreakerConfig;
import upic.consumer.config.ConsumerConfig;
import upic.consumer.model.LiftRideEvent;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;


/**
 * Optimized SQS Consumer implementation with dynamic thread scaling, performance monitoring,
 * and circuit breaker pattern for resilience.
 * This consumer processes lift ride events from SQS and stores them in a thread-safe data structure.
 */
public class SQSConsumer {
    // Configuration parameters
    private final int initialThreads;
    private final int maxThreads;
    private final String queueUrl;
    private final String region;
    private final int batchSize;
    private final int visibilityTimeoutSeconds;
    private final int waitTimeSeconds;

    // Services and utilities
    private final ExecutorService consumerExecutor;
    private final ScheduledExecutorService scheduledExecutor;
    private final AmazonSQS sqsClient;
    private final Gson gson;

    // Circuit breakers
    private final CircuitBreaker receiveMessageCircuitBreaker;
    private final CircuitBreaker deleteMessageCircuitBreaker;
    private final CircuitBreaker processMessageCircuitBreaker;

    // Data storage for skier events - this is our thread-safe hashmap required by the assignment
    private final ConcurrentHashMap<Integer, List<LiftRideEvent>> skierData = new ConcurrentHashMap<>();

    // Performance tracking
    private final LongAdder messagesProcessed = new LongAdder();
    private final LongAdder messagesPerSecond = new LongAdder();
    private final LongAdder processingErrors = new LongAdder();
    private final LongAdder batchProcessCount = new LongAdder();
    private final LongAdder circuitBreakerTrips = new LongAdder();
    private final LongAdder circuitBreakerSuccessfulRetries = new LongAdder();
    private final AtomicInteger activeThreads = new AtomicInteger(0);

    // Control flags
    private volatile boolean running = true;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    /**
     * Constructor using default configuration.
     */
    public SQSConsumer() {
        this(
                ConsumerConfig.INITIAL_THREAD_COUNT,
                ConsumerConfig.MAX_THREAD_COUNT,
                ConsumerConfig.SQS_QUEUE_URL,
                ConsumerConfig.AWS_REGION,
                ConsumerConfig.BATCH_SIZE,
                ConsumerConfig.VISIBILITY_TIMEOUT_SECONDS,
                ConsumerConfig.WAIT_TIME_SECONDS
        );
    }

    /**
     * Full constructor with all parameters.
     */
    public SQSConsumer(int initialThreads, int maxThreads, String queueUrl, String region,
                       int batchSize, int visibilityTimeoutSeconds, int waitTimeSeconds) {
        this.initialThreads = initialThreads;
        this.maxThreads = maxThreads;
        this.queueUrl = queueUrl;
        this.region = region;
        this.batchSize = batchSize;
        this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
        this.waitTimeSeconds = waitTimeSeconds;

        // Create thread pools with custom thread factory for better thread naming
        this.consumerExecutor = Executors.newFixedThreadPool(maxThreads, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("consumer-" + counter.incrementAndGet());
                return thread;
            }
        });

        this.scheduledExecutor = Executors.newScheduledThreadPool(2);

        // Setup AWS SQS client
        this.sqsClient = AmazonSQSClientBuilder.standard()
                .withRegion(region)
                .build();

        // Setup Gson for JSON parsing
        this.gson = new Gson();

        // Initialize circuit breakers
        CircuitBreakerConfig receiveConfig = new CircuitBreakerConfig(
                "SQS-Receive",
                ConsumerConfig.CIRCUIT_FAILURE_THRESHOLD,
                ConsumerConfig.CIRCUIT_RESET_TIMEOUT_MS
        );
        this.receiveMessageCircuitBreaker = new CircuitBreaker(receiveConfig, circuitBreakerTrips, circuitBreakerSuccessfulRetries);

        CircuitBreakerConfig deleteConfig = new CircuitBreakerConfig(
                "SQS-Delete",
                ConsumerConfig.CIRCUIT_FAILURE_THRESHOLD,
                ConsumerConfig.CIRCUIT_RESET_TIMEOUT_MS
        );
        this.deleteMessageCircuitBreaker = new CircuitBreaker(deleteConfig, circuitBreakerTrips, circuitBreakerSuccessfulRetries);

        CircuitBreakerConfig processConfig = new CircuitBreakerConfig(
                "Message-Processing",
                ConsumerConfig.CIRCUIT_FAILURE_THRESHOLD,
                ConsumerConfig.CIRCUIT_RESET_TIMEOUT_MS
        );
        this.processMessageCircuitBreaker = new CircuitBreaker(processConfig, circuitBreakerTrips, circuitBreakerSuccessfulRetries);
    }

    /**
     * Start the consumer with initial threads.
     */
    public void start() {
        System.out.println("Starting SQS Consumer with " + initialThreads + " initial threads");
        System.out.println("Configuration:");
        System.out.println(" - Queue URL: " + queueUrl);
        System.out.println(" - Region: " + region);
        System.out.println(" - Initial Threads: " + initialThreads);
        System.out.println(" - Max Threads: " + maxThreads);
        System.out.println(" - Batch Size: " + batchSize);
        System.out.println(" - Visibility Timeout: " + visibilityTimeoutSeconds + "s");
        System.out.println(" - Long Polling Wait Time: " + waitTimeSeconds + "s");
        System.out.println(" - Circuit Breaker Failure Threshold: " + ConsumerConfig.CIRCUIT_FAILURE_THRESHOLD);
        System.out.println(" - Circuit Breaker Reset Timeout: " + ConsumerConfig.CIRCUIT_RESET_TIMEOUT_MS + "ms");

        // Start background monitoring tasks
        startStatsReporting();
        startQueueMonitoring();
        startCircuitBreakerMonitoring();

        // Start consumer threads
        for (int i = 0; i < initialThreads; i++) {
            startConsumerThread(i);
        }
    }

    /**
     * Start a single consumer thread.
     */
    private void startConsumerThread(int threadId) {
        consumerExecutor.submit(() -> {
            System.out.println("Consumer thread #" + threadId + " started");
            activeThreads.incrementAndGet();
            try {
                consumeMessages(threadId);
            } finally {
                activeThreads.decrementAndGet();
            }
        });
    }

    /**
     * Main message consumption loop for a single thread.
     */
    private void consumeMessages(int threadId) {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                // Use long polling to reduce empty responses
                ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                        .withQueueUrl(queueUrl)
                        .withMaxNumberOfMessages(batchSize)
                        .withVisibilityTimeout(visibilityTimeoutSeconds)
                        .withWaitTimeSeconds(waitTimeSeconds);

                // Receive messages with circuit breaker protection
                List<Message> messages = receiveMessageCircuitBreaker.execute(() ->
                        sqsClient.receiveMessage(receiveRequest).getMessages()
                );

                if (!messages.isEmpty()) {
                    // Record batch processing
                    batchProcessCount.increment();

                    // Process messages and collect delete entries
                    List<DeleteMessageBatchRequestEntry> deleteEntries = new ArrayList<>();

                    for (Message message : messages) {
                        try {
                            // Process message with circuit breaker
                            processMessageWithCircuitBreaker(message, deleteEntries);
                        } catch (Exception e) {
                            processingErrors.increment();
                            System.err.println("Thread #" + threadId + " error processing message: " + e.getMessage());
                        }
                    }

                    // Delete processed messages in a batch with circuit breaker protection
                    if (!deleteEntries.isEmpty()) {
                        try {
                            deleteMessageCircuitBreaker.execute(() -> {
                                sqsClient.deleteMessageBatch(new DeleteMessageBatchRequest(queueUrl, deleteEntries));
                                return null; // Void result
                            });
                        } catch (Exception e) {
                            System.err.println("Error deleting message batch: " + e.getMessage());
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Thread #" + threadId + " error receiving messages: " + e.getMessage());
                // Pause briefly to prevent high CPU usage in case of continuous errors
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * Process a single message with circuit breaker protection.
     */
    private void processMessageWithCircuitBreaker(Message message, List<DeleteMessageBatchRequestEntry> deleteEntries) {
        try {
            processMessageCircuitBreaker.execute(() -> {
                // Parse JSON message body
                LiftRideEvent liftRideEvent = gson.fromJson(message.getBody(), LiftRideEvent.class);

                // Process message and update skier data
                processMessage(liftRideEvent);

                // Add to delete batch
                deleteEntries.add(new DeleteMessageBatchRequestEntry(
                        UUID.randomUUID().toString(),
                        message.getReceiptHandle()
                ));

                // Update statistics
                messagesProcessed.increment();
                messagesPerSecond.increment();

                return null; // Void result
            });
        } catch (Exception e) {
            processingErrors.increment();
            throw e; // Re-throw to be handled by the outer catch
        }
    }

    /**
     * Process a single lift ride event and store it in the skier data map.
     */
    private void processMessage(LiftRideEvent event) {
        int skierId = event.getSkierId();

        // Use computeIfAbsent to ensure we have a thread-safe list for each skier
        // This is our thread-safe hash map implementation required by the assignment
        skierData.computeIfAbsent(skierId, k ->
                Collections.synchronizedList(new ArrayList<>())
        ).add(event);
    }

    /**
     * Start periodic statistics reporting.
     */
    private void startStatsReporting() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                long processed = messagesProcessed.sum();
                long processedPerSecond = messagesPerSecond.sumThenReset();
                long errors = processingErrors.sum();
                long batchCount = batchProcessCount.sum();
                int numSkiers = skierData.size();
                int active = activeThreads.get();

                System.out.println("\n=== Performance Statistics - " +
                        LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME) + " ===");
                System.out.println("Active consumer threads: " + active);
                System.out.println("Messages processed per second: " + processedPerSecond);
                System.out.println("Total messages processed: " + processed);
                System.out.println("Processing errors: " + errors);
                System.out.println("Batch operations: " + batchCount);
                System.out.println("Distinct skiers tracked: " + numSkiers);

                // Memory usage
                Runtime runtime = Runtime.getRuntime();
                long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
                long totalMemory = runtime.totalMemory() / (1024 * 1024);
                long maxMemory = runtime.maxMemory() / (1024 * 1024);

                System.out.println("Memory usage: " + usedMemory + "MB / " + totalMemory +
                        "MB (Max: " + maxMemory + "MB)");

                System.out.println("=====================================");
            } catch (Exception e) {
                System.err.println("Error in stats reporting: " + e.getMessage());
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * Start periodic monitoring of circuit breakers.
     */
    private void startCircuitBreakerMonitoring() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                long trips = circuitBreakerTrips.sum();
                long successfulRetries = circuitBreakerSuccessfulRetries.sum();

                System.out.println("\n=== Circuit Breaker Status ===");
                System.out.println("Receive Circuit Breaker: " + receiveMessageCircuitBreaker.getState());
                System.out.println("Delete Circuit Breaker: " + deleteMessageCircuitBreaker.getState());
                System.out.println("Process Circuit Breaker: " + processMessageCircuitBreaker.getState());
                System.out.println("Total Circuit Trips: " + trips);
                System.out.println("Successful Retries: " + successfulRetries);
                System.out.println("============================");
            } catch (Exception e) {
                System.err.println("Error in circuit breaker monitoring: " + e.getMessage());
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    /**
     * Start periodic queue monitoring and auto-scaling.
     */
    private void startQueueMonitoring() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                // Get queue attributes
                Map<String, String> attributes = sqsClient.getQueueAttributes(queueUrl,
                        List.of("ApproximateNumberOfMessages",
                                "ApproximateNumberOfMessagesNotVisible")).getAttributes();

                int messagesInQueue = Integer.parseInt(attributes.get("ApproximateNumberOfMessages"));
                int messagesInFlight = Integer.parseInt(attributes.get("ApproximateNumberOfMessagesNotVisible"));

                System.out.println("\n=== Queue Statistics ===");
                System.out.println("Messages in queue: " + messagesInQueue);
                System.out.println("Messages in flight: " + messagesInFlight);
                System.out.println("======================");

                // Auto-scaling logic based on queue depth
                adjustThreadCount(messagesInQueue);

            } catch (Exception e) {
                System.err.println("Error monitoring queue: " + e.getMessage());
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    /**
     * Dynamically adjust thread count based on queue depth.
     */
    private void adjustThreadCount(int messagesInQueue) {
        int currentThreads = activeThreads.get();

        if (messagesInQueue > ConsumerConfig.HIGH_QUEUE_THRESHOLD && currentThreads < maxThreads) {
            // Queue is building up, add more threads if we're below max
            int threadsToAdd = Math.min(
                    ConsumerConfig.THREADS_TO_ADD,
                    maxThreads - currentThreads
            );
            System.out.println("Adding " + threadsToAdd + " consumer threads due to high queue depth");

            for (int i = 0; i < threadsToAdd; i++) {
                startConsumerThread(-1); // -1 indicates a dynamically added thread
            }
        } else if (messagesInQueue < ConsumerConfig.LOW_QUEUE_THRESHOLD && currentThreads > initialThreads) {
            // Queue is nearly empty, we can reduce threads
            // This is handled passively - we don't forcibly stop threads, but just note that we could
            // use fewer threads. This avoids interrupting work in progress.
            System.out.println("Queue depth is low. Could reduce by " +
                    Math.min(ConsumerConfig.THREADS_TO_REMOVE, currentThreads - initialThreads) +
                    " threads when current work completes.");
        }
    }

    /**
     * Gracefully shut down the consumer.
     */
    public void shutdown() {
        System.out.println("Shutting down consumer...");
        running = false;

        // Shutdown thread pools
        scheduledExecutor.shutdown();
        consumerExecutor.shutdown();

        try {
            if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
            if (!consumerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                consumerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutor.shutdownNow();
            consumerExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Close SQS client
        if (sqsClient != null) {
            sqsClient.shutdown();
        }

        System.out.println("Consumer shutdown complete, processed " + messagesProcessed.sum() + " messages total");
        shutdownLatch.countDown();
    }

    /**
     * Get the total number of messages processed.
     */
    public long getTotalMessagesProcessed() {
        return messagesProcessed.sum();
    }

    /**
     * Get the number of distinct skiers tracked.
     */
    public int getNumSkiers() {
        return skierData.size();
    }

    /**
     * Get the thread-safe map of skier data.
     */
    public ConcurrentHashMap<Integer, List<LiftRideEvent>> getSkierData() {
        return skierData;
    }

    /**
     * Wait for the consumer to shut down.
     */
    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }

    /**
     * Main method to run the consumer as a standalone application.
     */
    public static void main(String[] args) {
        // Create and start the consumer
        SQSConsumer consumer = new SQSConsumer();
        consumer.start();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Received shutdown signal...");
            consumer.shutdown();
        }));

        // Keep main thread alive
        try {
            consumer.awaitShutdown();
        } catch (InterruptedException e) {
            consumer.shutdown();
            Thread.currentThread().interrupt();
        }
    }
}