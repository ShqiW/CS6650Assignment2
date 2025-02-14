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

public class RequestSender implements Runnable {
  private final BlockingQueue<LiftRideEvent> queue;
  private final int requestCount;
  private final CountDownLatch latch;
  private final HttpClient client;
  private final Gson gson = new Gson();
  private final AtomicInteger successCount;
  private final AtomicInteger failureCount;

  public RequestSender(BlockingQueue<LiftRideEvent> queue, int requestCount,
      CountDownLatch latch, AtomicInteger successCount,
      AtomicInteger failureCount) {
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
//      System.out.println("Thread started, planning to send " + requestCount + " requests");
      for (int i = 0; i < requestCount; i++) {
        LiftRideEvent event = queue.take();
//        System.out.println("Sending request " + (i + 1) + " of " + requestCount);
        boolean success = sendRequest(event);
        if (success) {
          successCount.incrementAndGet();
//          System.out.println("Request " + (i + 1) + " succeeded");
        } else {
          failureCount.incrementAndGet();
//          System.out.println("Request " + (i + 1) + " failed");
        }
      }
    } catch (InterruptedException e) {
      System.out.println("Thread interrupted: " + e.getMessage());
      Thread.currentThread().interrupt();
    } finally {
      System.out.println("Thread completed");
      latch.countDown();
    }
  }

  private boolean sendRequest(LiftRideEvent event) {
    String json = gson.toJson(event);
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(ClientConfig.BASE_URL))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(json))
        .build();

    for (int attempt = 0; attempt < ClientConfig.MAX_RETRY_ATTEMPTS; attempt++) {
      try {
//        System.out.println("Attempt " + (attempt + 1) + " of " + ClientConfig.MAX_RETRY_ATTEMPTS);
        HttpResponse<String> response =
            client.send(request, HttpResponse.BodyHandlers.ofString());

//        System.out.println("Response status code: " + response.statusCode());

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

