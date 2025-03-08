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
import java.util.concurrent.atomic.LongAdder;

public class RequestSender implements Runnable {
  private final BlockingQueue<LiftRideEvent> queue;
  private final int requestCount;
  private final CountDownLatch latch;
  private final HttpClient client;
  private final Gson gson = new Gson();
  private final LongAdder successCount;
  private final LongAdder failureCount;

  public RequestSender(BlockingQueue<LiftRideEvent> queue, int requestCount,
      CountDownLatch latch, LongAdder successCount,
      LongAdder failureCount) {
    this.queue = queue;
    this.requestCount = requestCount;
    this.latch = latch;
    this.successCount = successCount;
    this.failureCount = failureCount;
    this.client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();
  }
  @Override
  public void run() {
    try {
      for (int i = 0; i < requestCount; i++) {
        LiftRideEvent event = queue.take();
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
    // Build the correct URL path with the required format
    String url = String.format("%s/resorts/%d/seasons/%d/days/%d/skiers/%d",
            ClientConfig.BASE_URL,
            event.getResortId(),
            event.getSeasonId(),
            event.getDayId(),
            event.getSkierId());

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(json))
        .build();

    for (int attempt = 0; attempt < ClientConfig.MAX_RETRY_ATTEMPTS; attempt++) {
      try {
        HttpResponse<String> response =
            client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 201 || response.statusCode() == 200) {
          return true;
        }
        if (response.statusCode() >= 400 && response.statusCode() < 500) {
          System.out.println("Client error: " + response.body());
          return false;
        }
      } catch (Exception e) {
        System.out.println("Request failed with error: " + e.getMessage());
        if (attempt == ClientConfig.MAX_RETRY_ATTEMPTS - 1) {
          return false;
        }
      }
    }
    return false;
  }
}
