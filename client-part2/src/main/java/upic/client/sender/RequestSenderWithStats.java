package upic.client.sender;

import com.google.gson.Gson;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import upic.client.config.ClientConfig;
import upic.client.model.LiftRideEvent;
import upic.client.model.Statistics;
import upic.client.util.StatisticsCollector;

public class RequestSenderWithStats implements Runnable {
  private final BlockingQueue<LiftRideEvent> queue;
  private final int requestCount;
  private final CountDownLatch latch;
  private final HttpClient client;
  private final Gson gson = new Gson();
  private final AtomicInteger successCount;
  private final AtomicInteger failureCount;
  private final StatisticsCollector statsCollector;

  public RequestSenderWithStats(BlockingQueue<LiftRideEvent> queue,
      int requestCount,
      CountDownLatch latch,
      AtomicInteger successCount,
      AtomicInteger failureCount,
      StatisticsCollector statsCollector) {
    this.queue = queue;
    this.requestCount = requestCount;
    this.latch = latch;
    this.successCount = successCount;
    this.failureCount = failureCount;
    this.statsCollector = statsCollector;
    this.client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();
  }

  @Override
  public void run() {
    try {
      for (int i = 0; i < requestCount; i++) {
        LiftRideEvent event = queue.take();
        sendRequestWithStats(event);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      latch.countDown();
    }
  }

  private void sendRequestWithStats(LiftRideEvent event) {
    String json = gson.toJson(event);
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(ClientConfig.BASE_URL))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(json))
        .build();

    long startTime = System.currentTimeMillis();
    int responseCode = -1;
    boolean success = false;

    for (int attempt = 0; attempt < ClientConfig.MAX_RETRY_ATTEMPTS; attempt++) {
      try {
        HttpResponse<String> response =
            client.send(request, HttpResponse.BodyHandlers.ofString());
        responseCode = response.statusCode();

        if (responseCode == 201 || responseCode == 200) {
          success = true;
          break;
        }
        if (responseCode >= 400 && responseCode < 500) {
          break;
        }
      } catch (Exception e) {
        if (attempt == ClientConfig.MAX_RETRY_ATTEMPTS - 1) {
          break;
        }
      }
    }

    long endTime = System.currentTimeMillis();

    statsCollector.recordStatistics(new Statistics(
        startTime,
        "POST",
        endTime - startTime,
        responseCode
    ));

    if (success) {
      successCount.incrementAndGet();
    } else {
      failureCount.incrementAndGet();
    }
  }
}
