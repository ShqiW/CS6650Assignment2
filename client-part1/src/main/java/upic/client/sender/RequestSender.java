package upic.client.sender;

import com.google.gson.Gson;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import upic.client.config.ClientConfig;
import upic.client.model.LiftRideEvent;

/**
 * Improved RequestSender with better error handling and connection management
 */
public class RequestSender implements Runnable {
  private final BlockingQueue<LiftRideEvent> queue;
  private final int requestCount;
  private final CountDownLatch latch;
  private final Gson gson = new Gson();
  private final LongAdder successCount;
  private final LongAdder failureCount;
  private final LongAdder timeoutCount = new LongAdder();
  private final LongAdder retryCount = new LongAdder();

  // Improved HTTP client with better connection management
  private final HttpClient client;

  // Backoff strategy for retries
  private final BackoffStrategy backoffStrategy;

  // Request throttling to avoid overwhelming the server
  private final RequestThrottler throttler;

  public RequestSender(BlockingQueue<LiftRideEvent> queue, int requestCount,
                               CountDownLatch latch, LongAdder successCount,
                               LongAdder failureCount) {
    this.queue = queue;
    this.requestCount = requestCount;
    this.latch = latch;
    this.successCount = successCount;
    this.failureCount = failureCount;

    // Create an optimized HttpClient
    this.client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(ClientConfig.CONNECTION_TIMEOUT_SECONDS))
            .executor(Executors.newFixedThreadPool(ClientConfig.CONNECTION_POOL_SIZE))
            .version(ClientConfig.USE_HTTP2 ? HttpClient.Version.HTTP_2 : HttpClient.Version.HTTP_1_1)
            .build();

    // Create exponential backoff strategy
    this.backoffStrategy = new ExponentialBackoffStrategy(
            ClientConfig.INITIAL_BACKOFF_MS,
            ClientConfig.MAX_BACKOFF_MS,
            ClientConfig.BACKOFF_MULTIPLIER
    );

    // Create request throttler
    this.throttler = new RequestThrottler(
            Integer.parseInt(System.getProperty(
                    "client.requestsPerSecond",
                    String.valueOf(ClientConfig.REQUESTS_PER_SECOND_LIMIT)
            ))
    );
  }

  @Override
  public void run() {
    try {
      for (int i = 0; i < requestCount; i++) {
        if(queue.isEmpty()){
          System.out.println("Queue is empty");
          return;
        }
        LiftRideEvent event = queue.take();

        // Apply request throttling
        throttler.throttle();

        boolean success = sendRequest(event);
        if (success) {
          successCount.increment();
        } else {
          failureCount.increment();
        }

      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      latch.countDown();
    }
  }

  private boolean sendRequest(LiftRideEvent event) {
    String json = gson.toJson(event);
    String url = String.format("%s/resorts/%d/seasons/%d/days/%d/skiers/%d",
            ClientConfig.BASE_URL,
            event.getResortId(),
            event.getSeasonId(),
            event.getDayId(),
            event.getSkierId());

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(ClientConfig.REQUEST_TIMEOUT_SECONDS))
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .build();

    // Reset backoff strategy
    backoffStrategy.reset();

    for (int attempt = 0; attempt < ClientConfig.MAX_RETRY_ATTEMPTS; attempt++) {
      try {
        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());

        // Check response status code
        if (response.statusCode() == 201 || response.statusCode() == 200) {
          return true;
        }

        // Differentiate between client and server errors
        if (response.statusCode() >= 400 && response.statusCode() < 500) {
          // Client error, no need to retry
          if (Math.random() < 0.01) { // Only log 1% of errors to reduce noise
            System.out.println("Client error (" + response.statusCode() + "): " + response.body());
          }
          return false;
        }

        // Server error, should retry
        if (Math.random() < 0.05) { // Only log 5% of errors
          System.out.println("Client-request Sender: Server error (" + response.statusCode() + "), retrying...");
        }

      } catch (java.net.http.HttpTimeoutException e) {
        // Handle timeout exceptions separately
        timeoutCount.increment();
        if (Math.random() < 0.01) { // Only log 1% of timeouts
          System.out.println("Request timed out, retrying...");
        }
      } catch (Exception e) {
        // Other exceptions
        if (Math.random() < 0.01) { // Only log 1% of exceptions
          System.out.println("Client request failed: " + e.getClass().getSimpleName() + " - " + e.getMessage());
        }
      }

      // Don't wait after the last attempt
      if (attempt < ClientConfig.MAX_RETRY_ATTEMPTS - 1) {
        retryCount.increment();
        try {
          // Use backoff strategy to calculate wait time
          long waitTime = backoffStrategy.nextBackoffMillis();
          TimeUnit.MILLISECONDS.sleep(waitTime);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }
    return false;
  }

  // Get statistics
  public long getTimeoutCount() {
    return timeoutCount.sum();
  }

  public long getRetryCount() {
    return retryCount.sum();
  }

  /**
   * Backoff strategy interface
   */
  interface BackoffStrategy {
    void reset();
    long nextBackoffMillis();
  }

  /**
   * Exponential backoff strategy implementation
   */
  static class ExponentialBackoffStrategy implements BackoffStrategy {
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final double multiplier;
    private long currentDelayMs;

    public ExponentialBackoffStrategy(long initialDelayMs, long maxDelayMs, double multiplier) {
      this.initialDelayMs = initialDelayMs;
      this.maxDelayMs = maxDelayMs;
      this.multiplier = multiplier;
      this.currentDelayMs = initialDelayMs;
    }

    @Override
    public void reset() {
      currentDelayMs = initialDelayMs;
    }

    @Override
    public long nextBackoffMillis() {
      long delay = currentDelayMs;
      // Calculate new delay for next time
      currentDelayMs = Math.min(
              maxDelayMs,
              (long)(currentDelayMs * multiplier)
      );
      // Add jitter to avoid thundering herd (0.5-1.5x)
      double jitter = 0.5 + Math.random();
      return (long)(delay * jitter);
    }
  }

  /**
   * Request throttler to limit request rate
   */
  static class RequestThrottler {
    private final int requestsPerSecond;
    private final long nanosPerRequest;
    private long lastRequestTime = System.nanoTime();

    public RequestThrottler(int requestsPerSecond) {
      this.requestsPerSecond = requestsPerSecond;
      this.nanosPerRequest = 1_000_000_000L / requestsPerSecond;
    }

    public void throttle() throws InterruptedException {
      long now = System.nanoTime();
      long elapsed = now - lastRequestTime;

      if (elapsed < nanosPerRequest) {
        long sleepTime = (nanosPerRequest - elapsed) / 1_000_000;
        if (sleepTime > 0) {
          Thread.sleep(sleepTime);
        }
      }

      lastRequestTime = System.nanoTime();
    }
  }
}