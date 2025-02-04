package upic.client.util;

import com.google.gson.Gson;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import upic.client.config.ClientConfig;
import upic.client.model.LiftRideEvent;
import upic.client.model.Statistics;

public class StatisticsCollector {
  private final ConcurrentLinkedQueue<Statistics> statistics = new ConcurrentLinkedQueue<>();

  public void recordStatistics(Statistics stats) {
    statistics.add(stats);
  }

  public void writeToCSV(String filename) {
    try (FileWriter writer = new FileWriter(filename)) {
      writer.write("StartTime,RequestType,Latency,ResponseCode\n");
      for (Statistics stat : statistics) {
        writer.write(String.format("%d,%s,%d,%d\n",
            stat.getStartTime(),
            stat.getRequestType(),
            stat.getLatency(),
            stat.getResponseCode()));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void printStatistics(long wallTime) {
    List<Long> latencies = new ArrayList<>();
    statistics.forEach(stat -> latencies.add(stat.getLatency()));

    DescriptiveStatistics stats = new DescriptiveStatistics();
    latencies.forEach(stats::addValue);

    System.out.println("\nDetailed Performance Statistics:");
    System.out.println("Mean Response Time: " + String.format("%.2f", stats.getMean()) + " ms");
    System.out.println("Median Response Time: " + String.format("%.2f", stats.getPercentile(50)) + " ms");
    System.out.println("99th Percentile Response Time: " + String.format("%.2f", stats.getPercentile(99)) + " ms");
    System.out.println("Min Response Time: " + String.format("%.2f", stats.getMin()) + " ms");
    System.out.println("Max Response Time: " + String.format("%.2f", stats.getMax()) + " ms");
    System.out.println("Throughput: " + String.format("%.2f", (statistics.size() * 1000.0 / wallTime)) + " requests/second");
  }
}

