package upic.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import upic.client.model.LiftRideEvent;
import upic.client.producer.EventGenerator;
import upic.client.sender.RequestSender;

public class SingleThreadTest {
  private static final int TEST_REQUESTS = 10000;
  private final BlockingQueue<LiftRideEvent> eventQueue;
  private final AtomicInteger successCount = new AtomicInteger(0);
  private final AtomicInteger failedCount = new AtomicInteger(0);

  public SingleThreadTest(){
    this.eventQueue = new LinkedBlockingQueue<>(1000);
  }

  public void runTest(){
    System.out.println("Starting single thread test ...");
    long startTime = System.currentTimeMillis();

    // start event generator
    EventGenerator generator = new EventGenerator(eventQueue);
    Thread generatorThread = new Thread(generator);
    generatorThread.start();

    // single thread test
    CountDownLatch latch = new CountDownLatch(1);
    Thread senderThread = new Thread(new RequestSender(
        eventQueue,
        TEST_REQUESTS,
        latch,
        successCount,
        failedCount
    ));
    senderThread.start();

    try{
      latch.await();
    } catch(InterruptedException e){
      Thread.currentThread().interrupt();
    }finally{
      generator.stop();
    };

    long endTime = System.currentTimeMillis();
    printResults(endTime-startTime);

  }

  private void printResults (long wallTime){
    System.out.println("\nBenchmark Results:");
    System.out.println("Total Request: "+ TEST_REQUESTS);
    System.out.println("Successful Requests: "+ successCount);
    System.out.println("Failed Requests: " + failedCount);
    System.out.println("Wall Time: "+ wallTime + "ms");
    double throughput = (TEST_REQUESTS*1000)/wallTime;
    System.out.println("Single Thread Throughput: " + String.format("%.2f", throughput) + " requests/second");
    System.out.println("Average Latency: " + String.format("%.2f", wallTime * 1.0 / TEST_REQUESTS) + " ms/request");
  }

  public static void main(String[] args){
    new SingleThreadTest().runTest();
  }
}
