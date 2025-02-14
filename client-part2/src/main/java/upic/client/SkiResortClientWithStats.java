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
import upic.client.sender.RequestSenderWithStats;
import upic.client.util.StatisticsCollector;
import upic.client.util.ThroughputChartGenerator;
import java.util.concurrent.atomic.LongAdder;

public class SkiResortClientWithStats {
  private final BlockingQueue<LiftRideEvent> eventQueue;
//  private final AtomicInteger successCount = new AtomicInteger(0);
//  private final AtomicInteger failureCount = new AtomicInteger(0);
  private final LongAdder successCount = new LongAdder();
  private final LongAdder failureCount = new LongAdder();
  private final ExecutorService executor;
  private final StatisticsCollector statsCollector;

  public SkiResortClientWithStats() {
    this.eventQueue = new LinkedBlockingQueue<>(ClientConfig.QUEUE_SIZE);
    this.executor = Executors.newCachedThreadPool();
    this.statsCollector = new StatisticsCollector();
  }

  public void start() {
    System.out.println("Starting client...");
    System.out.println("Configuration:");
    System.out.println(" - Initial Threads: " + ClientConfig.INITIAL_THREADS);

    long startTime = System.currentTimeMillis();

    // Start event generator
    EventGenerator generator = new EventGenerator(eventQueue);
    Thread generatorThread = new Thread(generator);
    generatorThread.start();

    // Initial phase with 32 threads
    CountDownLatch initialLatch = new CountDownLatch(ClientConfig.INITIAL_THREADS);
    for (int i = 0; i < ClientConfig.INITIAL_THREADS; i++) {
      executor.submit(new RequestSenderWithStats(
          eventQueue,
          ClientConfig.REQUESTS_PER_THREAD,
          initialLatch,
          successCount,
          failureCount,
          statsCollector
      ));
    }

    try {
      initialLatch.await();
      System.out.println("Initial phase completed");

      // Calculate remaining requests
      int completedRequests = ClientConfig.INITIAL_THREADS * ClientConfig.REQUESTS_PER_THREAD;
      int remainingRequests = ClientConfig.TOTAL_REQUESTS - completedRequests;

      if (remainingRequests > 0) {
        int optimalThreadCount = getOptimalThreadCount(remainingRequests);
        int requestsPerThread = remainingRequests / optimalThreadCount;

        System.out.println(" - Remaining Requests: " + remainingRequests);
        System.out.println(" - Additional Threads Used: " + optimalThreadCount);

        CountDownLatch remainingLatch = new CountDownLatch(optimalThreadCount);

        for (int i = 0; i < optimalThreadCount; i++) {
          executor.submit(new RequestSenderWithStats(
              eventQueue,
              requestsPerThread,
              remainingLatch,
              successCount,
              failureCount,
              statsCollector
          ));
        }

        remainingLatch.await();
        System.out.println("Remaining phase completed");
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      generator.stop();
      executor.shutdown();
    }

    long endTime = System.currentTimeMillis();
    long wallTime = endTime - startTime;

    printResults(wallTime);
    statsCollector.printStatistics(wallTime);
    statsCollector.writeToCSV("request_statistics.csv");
    ThroughputChartGenerator.generateThroughputChart("request_statistics.csv", "throughput_chart.png");
  }


  private int getOptimalThreadCount(int remainingRequests) {
    int processors = Runtime.getRuntime().availableProcessors();
    return Math.min(processors * 4, remainingRequests / 100);
  }

  private void printResults(long wallTime) {
    System.out.println("\nClient Part 2 Results:");
    System.out.println("Total Threads: " + ClientConfig.INITIAL_THREADS);
    System.out.println("Additional Threads Used: " + getOptimalThreadCount(ClientConfig.TOTAL_REQUESTS - (ClientConfig.INITIAL_THREADS * ClientConfig.REQUESTS_PER_THREAD)));
//    System.out.println("Successful Requests: " + successCount.get());
//    System.out.println("Failed Requests: " + failureCount.get());
    System.out.println("Successful Requests: " + successCount.sum());
    System.out.println("Failed Requests: " + failureCount.sum());
    System.out.println("Wall Time: " + wallTime + " ms");
  }

  public static void main(String[] args) {
    new SkiResortClientWithStats().start();
  }
}