package upic.consumer.config;

/**
 * Configuration class for the consumer component.
 * Centralizes all consumer-related parameters for easy tuning.
 */
public class ConsumerConfig {
    // AWS SQS configuration
    public static final String AWS_REGION = System.getProperty("aws.region", "us-west-2");
    public static final String SQS_QUEUE_URL = System.getProperty("aws.queueUrl",
            "https://sqs.us-west-2.amazonaws.com/145832436892/ski-lift-events");

    // Thread pool configuration
    public static final int INITIAL_THREAD_COUNT = Integer.parseInt(
            System.getProperty("consumer.initialThreads", "64"));
    public static final int MAX_THREAD_COUNT = Integer.parseInt(
            System.getProperty("consumer.maxThreads", "128"));

    // SQS receive configuration
    public static final int BATCH_SIZE = Integer.parseInt(
            System.getProperty("consumer.batchSize", "10"));
    public static final int VISIBILITY_TIMEOUT_SECONDS = Integer.parseInt(
            System.getProperty("consumer.visibilityTimeout", "30"));
    public static final int WAIT_TIME_SECONDS = Integer.parseInt(
            System.getProperty("consumer.waitTimeSeconds", "20"));

    // Data management configuration
    public static final int MAX_EVENTS_PER_SKIER = Integer.parseInt(
            System.getProperty("consumer.maxEventsPerSkier", "1000"));
    public static final String DATA_EXPORT_PATH = System.getProperty(
            "consumer.dataExportPath", "/tmp/skier_data");
    public static final long EXPORT_INTERVAL_MILLIS = Long.parseLong(
            System.getProperty("consumer.exportIntervalMillis", "300000"));

    // Performance monitoring
    public static final long STATS_INTERVAL_MS = Long.parseLong(
            System.getProperty("consumer.statsInterval", "60000"));

    // Auto-scaling configuration
    public static final int HIGH_QUEUE_THRESHOLD = Integer.parseInt(
            System.getProperty("consumer.highQueueThreshold", "10000"));
    public static final int LOW_QUEUE_THRESHOLD = Integer.parseInt(
            System.getProperty("consumer.lowQueueThreshold", "100"));
    public static final int THREADS_TO_ADD = Integer.parseInt(
            System.getProperty("consumer.threadsToAdd", "10"));
    public static final int THREADS_TO_REMOVE = Integer.parseInt(
            System.getProperty("consumer.threadsToRemove", "5"));

    // Circuit breaker configuration
    public static final int CIRCUIT_FAILURE_THRESHOLD = Integer.parseInt(
            System.getProperty("consumer.circuitFailureThreshold", "5"));
    public static final long CIRCUIT_RESET_TIMEOUT_MS = Long.parseLong(
            System.getProperty("consumer.circuitResetTimeoutMs", "10000"));

    /**
     * Calculate optimal thread count based on CPU cores and I/O intensity
     *
     * @return The recommended number of threads
     */
    public static int calculateOptimalThreadCount() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        // For I/O bound operations, we typically want more threads than CPU cores
        // A common formula is: threads = cores * (1 + wait_time / service_time)
        // Assuming a 1:10 ratio of service to wait time for SQS operations
        return availableProcessors * 11;
    }
}