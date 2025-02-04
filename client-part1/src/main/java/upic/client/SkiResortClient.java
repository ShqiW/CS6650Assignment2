package upic.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import upic.client.config.ClientConfig;
import upic.client.model.LiftRideEvent;
import upic.client.producer.EventGenerator;
import upic.client.sender.RequestSender;

public class SkiResortClient {
  private final BlockingQueue<LiftRideEvent> eventQueue;
  private final AtomicInteger successCount = new AtomicInteger(0);
  private final AtomicInteger failureCount = new AtomicInteger(0);
  private final ExecutorService executor;

  public SkiResortClient() {
    this.eventQueue = new LinkedBlockingQueue<>(ClientConfig.QUEUE_SIZE);
    this.executor = Executors.newCachedThreadPool();
  }

  public void start() {
//    System.out.println("Starting client...");
    long startTime = System.currentTimeMillis();

    // Start event generator
//    System.out.println("Starting event generator...");
    EventGenerator generator = new EventGenerator(eventQueue);
    Thread generatorThread = new Thread(generator);
    generatorThread.start();

    // Initial phase with 32 threads
//    System.out.println("Starting initial phase with " + ClientConfig.INITIAL_THREADS + " threads...");
    CountDownLatch initialLatch = new CountDownLatch(ClientConfig.INITIAL_THREADS);
    for (int i = 0; i < ClientConfig.INITIAL_THREADS; i++) {
      executor.submit(new RequestSender(
          eventQueue,
          ClientConfig.REQUESTS_PER_THREAD,
          initialLatch,
          successCount,
          failureCount
      ));
    }

    try {
//      System.out.println("Waiting for initial phase to complete...");
      initialLatch.await();
//      System.out.println("Initial phase completed");

      // Calculate remaining requests
      int completedRequests = ClientConfig.INITIAL_THREADS * ClientConfig.REQUESTS_PER_THREAD;
      int remainingRequests = ClientConfig.TOTAL_REQUESTS - completedRequests;
//      System.out.println("Remaining requests: " + remainingRequests);

      if (remainingRequests > 0) {
        int optimalThreadCount = getOptimalThreadCount(remainingRequests);
        int requestsPerThread = remainingRequests / optimalThreadCount;
//        System.out.println("Starting remaining phase with " + optimalThreadCount + " threads...");

        CountDownLatch remainingLatch = new CountDownLatch(optimalThreadCount);

        for (int i = 0; i < optimalThreadCount; i++) {
          executor.submit(new RequestSender(
              eventQueue,
              requestsPerThread,
              remainingLatch,
              successCount,
              failureCount
          ));
        }

//        System.out.println("Waiting for remaining phase to complete...");
        remainingLatch.await();
//        System.out.println("Remaining phase completed");
      }

    } catch (InterruptedException e) {
//      System.out.println("Client interrupted: " + e.getMessage());
      Thread.currentThread().interrupt();
    } finally {
//      System.out.println("Shutting down client...");
      generator.stop();
      executor.shutdown();
    }

    long endTime = System.currentTimeMillis();
    printResults(endTime - startTime);
  }
  private int getOptimalThreadCount(int remainingRequests) {
    int processors = Runtime.getRuntime().availableProcessors();
    return Math.min(processors * 4, remainingRequests / 100);
  }

  private void printResults(long wallTime) {
    System.out.println("\nClient Part 1 Results:");
    System.out.println("Total Threads: " + ClientConfig.INITIAL_THREADS);
    System.out.println("Successful Requests: " + successCount.get());
    System.out.println("Failed Requests: " + failureCount.get());
    System.out.println("Wall Time: " + wallTime + " ms");
    System.out.println("Throughput: " +
        String.format("%.2f", (ClientConfig.TOTAL_REQUESTS * 1000.0 / wallTime)) +
        " requests/second");
  }

  public static void main(String[] args) {
    new SkiResortClient().start();
  }
}