package upic.consumer;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SQSConsumer {
    private final int numThreads;
    private final ExecutorService executorService;
    private final AmazonSQS sqsClient;
    private final String queueUrl;
    private final Gson gson;

    // Thread-safe HashMap to store lift rides for each skier
    private final ConcurrentHashMap<Integer, List<LiftRideEvent>> skierData = new ConcurrentHashMap<>();

    // Counters for statistics
    private final AtomicLong messagesProcessed = new AtomicLong(0);
    private final AtomicLong messagesLastMinute = new AtomicLong(0);
    private final AtomicLong processingErrors = new AtomicLong(0);
    private volatile boolean running = true;

    public SQSConsumer(int numThreads, String queueUrl, String region) {
        this.numThreads = numThreads;
        this.executorService = Executors.newFixedThreadPool(numThreads);

        // Setup AWS SQS client
        this.sqsClient = AmazonSQSClientBuilder.standard()
                .withRegion(region)
                .build();
        this.queueUrl = queueUrl;
        this.gson = new Gson();

        // Start statistics reporting thread
        startStatsReporting();
    }

    public void start() {
        System.out.println("Starting " + numThreads + " consumer threads...");

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                System.out.println("Consumer thread #" + threadId + " started");
                consumeMessages(threadId);
            });
        }
    }

    private void consumeMessages(int threadId) {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                // Setup receive message request
                ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                        .withQueueUrl(queueUrl)
                        .withMaxNumberOfMessages(10)  // Receive up to 10 messages at a time
                        .withWaitTimeSeconds(10);     // Use long polling to reduce empty requests

                // Receive messages
                List<Message> messages = sqsClient.receiveMessage(receiveRequest).getMessages();

                if (!messages.isEmpty()) {
                    for (Message message : messages) {
                        try {
                            // Parse JSON message body
                            LiftRideEvent liftRideEvent = gson.fromJson(message.getBody(), LiftRideEvent.class);

                            // Process message and update skier data
                            processMessage(liftRideEvent);

                            // Delete message from queue
                            sqsClient.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));

                            // Update statistics
                            messagesProcessed.incrementAndGet();
                            messagesLastMinute.incrementAndGet();
                        } catch (Exception e) {
                            processingErrors.incrementAndGet();
                            System.err.println("Thread #" + threadId + " error processing message: " + e.getMessage());
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

    private void processMessage(LiftRideEvent event) {
        int skierId = event.getSkierId();

        // Use computeIfAbsent to ensure we have a thread-safe list for each skier
        skierData.computeIfAbsent(skierId, k -> Collections.synchronizedList(new ArrayList<>()))
                .add(event);
    }

    private void startStatsReporting() {
        Thread statsThread = new Thread(() -> {
            long lastTimestamp = System.currentTimeMillis();

            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(60000); // Report stats every minute

                    long current = System.currentTimeMillis();
                    long timeElapsed = (current - lastTimestamp) / 1000;
                    lastTimestamp = current;

                    long processed = messagesLastMinute.getAndSet(0);
                    long totalProcessed = messagesProcessed.get();
                    long errors = processingErrors.get();

                    System.out.println("\n=== Performance Statistics - " + new java.util.Date() + " ===");
                    System.out.println("Messages processed in last " + timeElapsed + " seconds: " + processed +
                            " (" + String.format("%.2f", processed / (double)timeElapsed) + " per second)");
                    System.out.println("Total messages processed: " + totalProcessed);
                    System.out.println("Total processing errors: " + errors);
                    System.out.println("Number of skiers: " + skierData.size());
                    System.out.println("Memory usage: " +
                            (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024) +
                            "MB / " + Runtime.getRuntime().totalMemory() / (1024 * 1024) + "MB");

                    // Get queue attributes
                    try {
                        Map<String, String> attributes = sqsClient.getQueueAttributes(queueUrl,
                                List.of("ApproximateNumberOfMessages",
                                        "ApproximateNumberOfMessagesNotVisible")).getAttributes();

                        System.out.println("Messages in queue: " + attributes.get("ApproximateNumberOfMessages"));
                        System.out.println("Messages in flight: " + attributes.get("ApproximateNumberOfMessagesNotVisible"));
                    } catch (Exception e) {
                        System.err.println("Error getting queue attributes: " + e.getMessage());
                    }

                    System.out.println("=====================================");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        statsThread.setDaemon(true);
        statsThread.start();
    }

    public void shutdown() {
        System.out.println("Shutting down consumer...");
        running = false;
        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.println("Consumer shutdown complete, processed " + messagesProcessed.get() + " messages total");
    }

    public ConcurrentHashMap<Integer, List<LiftRideEvent>> getSkierData() {
        return skierData;
    }

    public long getTotalMessagesProcessed() {
        return messagesProcessed.get();
    }

    // Sample entry point
    public static void main(String[] args) {
        // Replace with your actual SQS URL and region
        String queueUrl = "https://sqs.us-west-2.amazonaws.com/941377132918/ski-lift-events";
        String region = "us-west-2";
        int numThreads = 16; // Adjust based on your instance's vCPU count

        SQSConsumer consumer = new SQSConsumer(numThreads, queueUrl, region);
        consumer.start();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Received shutdown signal...");
            consumer.shutdown();
        }));

        // Keep main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            consumer.shutdown();
        }
    }
}