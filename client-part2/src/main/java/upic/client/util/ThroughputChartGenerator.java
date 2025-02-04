package upic.client.util;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;

import java.io.*;
import java.util.*;

public class ThroughputChartGenerator {
  private final String csvFilePath;
  private final List<Record> records;

  private static class Record {
    final long startTime;
    final long latency;
    final String requestType;
    final int responseCode;

    Record(long startTime, String requestType, long latency, int responseCode) {
      this.startTime = startTime;
      this.requestType = requestType;
      this.latency = latency;
      this.responseCode = responseCode;
    }
  }

  public ThroughputChartGenerator(String csvFilePath) {
    this.csvFilePath = csvFilePath;
    this.records = new ArrayList<>();
  }

  public void loadData() throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
      String line;
      reader.readLine();

      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(",");
        try {
          long startTime = Long.parseLong(parts[0]);
          String requestType = parts[1];
          long latency = Long.parseLong(parts[2]);
          int responseCode = Integer.parseInt(parts[3]);

          records.add(new Record(startTime, requestType, latency, responseCode));
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
          System.err.println("Error parsing line: " + line);
        }
      }
    }

    // Sort records by start time
    records.sort(Comparator.comparingLong(r -> r.startTime));
  }

  public void generateChart(String outputPath) throws IOException {
    if (records.isEmpty()) {
      throw new IllegalStateException("No data loaded. Call loadData() first.");
    }

    Map<Long, Integer> throughputBySecond = new TreeMap<>();
    long firstRequestTime = records.get(0).startTime;

    for (Record record : records) {

      long second = (record.startTime - firstRequestTime) / 1000;
      throughputBySecond.merge(second, 1, Integer::sum);
    }

    TimeSeries series = new TimeSeries("Throughput");
    for (Map.Entry<Long, Integer> entry : throughputBySecond.entrySet()) {
      series.add(new Second(new Date(firstRequestTime + entry.getKey() * 1000)),
          entry.getValue().doubleValue());
    }

    TimeSeriesCollection dataset = new TimeSeriesCollection(series);

    JFreeChart chart = ChartFactory.createTimeSeriesChart(
        "Throughput Over Time",
        "Time",
        "Requests per Second",
        dataset,
        true,
        true,
        false
    );

    // Customize the chart
    XYPlot plot = (XYPlot) chart.getPlot();
    NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
    rangeAxis.setAutoRangeIncludesZero(true);

    // Save the chart
    ChartUtils.saveChartAsPNG(new File(outputPath), chart, 1200, 800);
    System.out.println("Chart generated: " + outputPath);
  }

  public static void generateThroughputChart(String csvFilePath, String outputPath) {
    try {
      ThroughputChartGenerator generator = new ThroughputChartGenerator(csvFilePath);
      generator.loadData();
      generator.generateChart(outputPath);
    } catch (IOException e) {
      System.err.println("Error generating throughput chart: " + e.getMessage());
      e.printStackTrace();
    }
  }
}