package upic.server;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.google.gson.Gson;
import upic.server.config.SQSAuthConfig;
import upic.server.model.ErrorResponse;
import upic.server.model.LiftRideEvent;
import upic.server.model.SuccessResponse;
import upic.server.config.ServerConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet(urlPatterns = "/skiers/*", asyncSupported = true)
public class SkierServlet extends HttpServlet {
  private final Gson gson = new Gson();
  private AmazonSQS sqsClient;
  private String queueUrl;
  private ExecutorService executorService;
  private ScheduledExecutorService scheduledExecutor;

  // Performance metrics
  private final LongAdder requestsReceived = new LongAdder();
  private final LongAdder requestsProcessed = new LongAdder();
  private final LongAdder requestsFailed = new LongAdder();
  private final LongAdder messagesSent = new LongAdder();
  private final LongAdder sqsErrors = new LongAdder();
  private final LongAdder validationErrors = new LongAdder();

  // Message batching
  private final ConcurrentLinkedQueue<BatchItem> messageBatch = new ConcurrentLinkedQueue<>();
  private static final int BATCH_SIZE = 10;
  private static final long BATCH_FLUSH_INTERVAL_MS = 100;

  // Health metrics
  private final Runtime runtime = Runtime.getRuntime();
  private static final long STATS_INTERVAL_MS = 60000; // 1 minute

  @Override
  public void init() throws ServletException {
    super.init();

    try {
      // Load configuration from system properties or use defaults
//      String region = ServerConfig.AWS_REGION;
//      queueUrl = ServerConfig.SQS_QUEUE_URL;
      int corePoolSize = ServerConfig.CORE_POOL_SIZE;

      if (queueUrl == null || queueUrl.isEmpty()) {
        getServletContext().log("Queue URL not specified, cannot initialize SQS client");
        throw new ServletException("Queue URL not specified");
      }

//      // Create SQS client
//      sqsClient = AmazonSQSClientBuilder.standard()
//              .withRegion(Regions.fromName(region))
//              .build();
      sqsClient = SQSAuthConfig.getSQSClient();
      queueUrl = SQSAuthConfig.getQueueUrl();
      // create SQS client without IAM


      // Initialize thread pools - use optimal thread count
      int optimalThreads = ServerConfig.getOptimalThreadCount();
      getServletContext().log("Using optimal thread count: " + optimalThreads);

      executorService = Executors.newFixedThreadPool(optimalThreads);
      scheduledExecutor = Executors.newScheduledThreadPool(2);

      // Schedule batch message processor
      scheduledExecutor.scheduleAtFixedRate(
              this::flushMessageBatch,
              BATCH_FLUSH_INTERVAL_MS,
              BATCH_FLUSH_INTERVAL_MS,
              TimeUnit.MILLISECONDS
      );

      // Schedule stats reporting
      scheduledExecutor.scheduleAtFixedRate(
              this::reportStats,
              STATS_INTERVAL_MS,
              STATS_INTERVAL_MS,
              TimeUnit.MILLISECONDS
      );

      getServletContext().log("SQS client and thread pools created. Queue URL: " + queueUrl);

    } catch (Exception e) {
      getServletContext().log("Initialization failed: " + e.getMessage());
      throw new ServletException("Cannot initialize servlet", e);
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res)
          throws IOException {
    res.setContentType("application/json");
    PrintWriter out = res.getWriter();

    // Create JSON response
    String jsonResponse = "{"
            + "\"message\": \"Welcome to Ski Resort API\","
            + "\"usage\": \"Please use POST method to submit ski lift rides\","
            + "\"example\": {"
            + "    \"skierId\": 123,"
            + "    \"resortId\": 5,"
            + "    \"liftId\": 15,"
            + "    \"seasonId\": 2025,"
            + "    \"dayId\": 1,"
            + "    \"time\": 217"
            + "  }"
            + "}";

    out.print(jsonResponse);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
          throws ServletException, IOException {
    // Increment request counter
    requestsReceived.increment();

    // Start async processing
    final AsyncContext asyncContext = req.startAsync();
    asyncContext.setTimeout(ServerConfig.ASYNC_TIMEOUT); // configurable timeout

    // Process request asynchronously
    executorService.submit(() -> {
      try {
        processRequest(asyncContext);
        requestsProcessed.increment();
      } catch (Exception e) {
        handleProcessingError(asyncContext, e);
        requestsFailed.increment();
      }
    });
  }

  private void processRequest(AsyncContext asyncContext) throws IOException {
    HttpServletRequest req = (HttpServletRequest) asyncContext.getRequest();
    HttpServletResponse resp = (HttpServletResponse) asyncContext.getResponse();
    resp.setContentType("application/json");
    PrintWriter writer = resp.getWriter();

    String requestId = UUID.randomUUID().toString();

    // Validate URL path
    String urlPath = req.getPathInfo();
    int urlResortId = 0, urlSeasonId = 0, urlDayId = 0, urlSkierId = 0;

    if (urlPath != null && urlPath.startsWith("/")) {
      String[] parts = urlPath.substring(1).split("/");
      if (parts.length == 8) {
        try {
          urlResortId = Integer.parseInt(parts[1]);
          urlSeasonId = Integer.parseInt(parts[3]);
          urlDayId = Integer.parseInt(parts[5]);
          urlSkierId = Integer.parseInt(parts[7]);
        } catch (NumberFormatException ignored) {
          // Will be handled by isUrlValid
        }
      }
    }

    boolean isValid = isUrlValid(urlPath);
    if (!isValid) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ErrorResponse error = new ErrorResponse("Invalid URL path", HttpServletResponse.SC_BAD_REQUEST);
      writer.write(gson.toJson(error));
      validationErrors.increment();
      asyncContext.complete();
      return;
    }

    try {
      // Parse and validate JSON payload
      BufferedReader reader = req.getReader();
      LiftRideEvent liftRide = gson.fromJson(reader, LiftRideEvent.class);

      if (!isValidLiftRide(liftRide)) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        ErrorResponse error = new ErrorResponse("Invalid lift ride data", HttpServletResponse.SC_BAD_REQUEST);
        writer.write(gson.toJson(error));
        validationErrors.increment();
        asyncContext.complete();
        return;
      }

      // Validate URL parameters match JSON body
      if (liftRide.getResortId() != urlResortId ||
              liftRide.getSeasonId() != urlSeasonId ||
              liftRide.getDayId() != urlDayId ||
              liftRide.getSkierId() != urlSkierId) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        ErrorResponse error = new ErrorResponse(
                "Mismatch between URL parameters and JSON body",
                HttpServletResponse.SC_BAD_REQUEST);
        writer.write(gson.toJson(error));
        validationErrors.increment();
        asyncContext.complete();
        return;
      }

      // Check queue URL
      if (queueUrl == null || queueUrl.isEmpty()) {
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        ErrorResponse error = new ErrorResponse(
                "Queue URL not configured",
                HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        writer.write(gson.toJson(error));
        asyncContext.complete();
        return;
      }

      // Send message to SQS using batch mechanism
      String messageBody = gson.toJson(liftRide);
      BatchItem batchItem = new BatchItem(requestId, messageBody);
      messageBatch.add(batchItem);

      // If batch size reached, trigger flush in separate thread
      if (messageBatch.size() >= ServerConfig.BATCH_SIZE) {
        triggerBatchFlush();
      }

      // Return success response
      resp.setStatus(HttpServletResponse.SC_CREATED);
      SuccessResponse success = new SuccessResponse("Lift ride recorded successfully, request ID: " + requestId);
      writer.write(gson.toJson(success));

      // Complete async context
      asyncContext.complete();

    } catch (Exception e) {
      // Handle JSON parsing or other exceptions
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ErrorResponse error = new ErrorResponse("Invalid request format: " + e.getMessage(),
              HttpServletResponse.SC_BAD_REQUEST);
      writer.write(gson.toJson(error));
      validationErrors.increment();
      asyncContext.complete();
    }
  }

  private void handleProcessingError(AsyncContext asyncContext, Exception e) {
    try {
      HttpServletResponse resp = (HttpServletResponse) asyncContext.getResponse();
      resp.setContentType("application/json");
      PrintWriter writer = resp.getWriter();

      System.err.println("Error processing request: " + e.getMessage());
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      ErrorResponse error = new ErrorResponse("Server error: " + e.getMessage(),
              HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      writer.write(gson.toJson(error));
    } catch (IOException ioe) {
      System.err.println("Error writing error response: " + ioe.getMessage());
    } finally {
      asyncContext.complete();
    }
  }

  private void triggerBatchFlush() {
    // Schedule immediate batch flush in a separate thread
    scheduledExecutor.execute(this::flushMessageBatch);
  }

  private synchronized void flushMessageBatch() {
    if (messageBatch.isEmpty()) {
      return;
    }

    try {
      List<SendMessageBatchRequestEntry> entries = new ArrayList<>(BATCH_SIZE);
      int count = 0;

      // Take up to BATCH_SIZE messages
      while (!messageBatch.isEmpty() && count < BATCH_SIZE) {
        BatchItem item = messageBatch.poll();
        if (item != null) {
          entries.add(new SendMessageBatchRequestEntry(item.id, item.messageBody));
          count++;
        }
      }

      if (!entries.isEmpty()) {
        // Send batch request
        SendMessageBatchRequest batchRequest = new SendMessageBatchRequest(queueUrl, entries);
        sqsClient.sendMessageBatch(batchRequest);
        messagesSent.add(entries.size());

        if (Math.random() < 0.01) { // Log 1% of batch sends
          getServletContext().log("Sent batch of " + entries.size() + " messages to SQS");
        }
      }
    } catch (Exception e) {
      // Log error and add messages back to queue
      getServletContext().log("Error sending batch to SQS: " + e.getMessage());
      sqsErrors.increment();
    }
  }

  private void sendIndividualMessage(String messageBody) {
    try {
      SendMessageRequest sendMsgRequest = new SendMessageRequest()
              .withQueueUrl(queueUrl)
              .withMessageBody(messageBody);

      SendMessageResult result = sqsClient.sendMessage(sendMsgRequest);
      messagesSent.increment();

      if (Math.random() < 0.001) { // Log 0.1% of messages
        getServletContext().log("Message sent to SQS successfully, message ID: " + result.getMessageId());
      }
    } catch (Exception e) {
      getServletContext().log("Error sending message to SQS: " + e.getMessage());
      sqsErrors.increment();
    }
  }

  private void reportStats() {
    long received = requestsReceived.sum();
    long processed = requestsProcessed.sum();
    long failed = requestsFailed.sum();
    long sent = messagesSent.sum();
    long sqs_errors = sqsErrors.sum();
    long validation_errors = validationErrors.sum();
    long usedMemoryMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    long maxMemoryMB = runtime.maxMemory() / (1024 * 1024);

    getServletContext().log(String.format(
            "STATS: Received=%d, Processed=%d, Failed=%d, Sent=%d, SQS_Errors=%d, Validation_Errors=%d, Memory=%dMB/%dMB",
            received, processed, failed, sent, sqs_errors, validation_errors, usedMemoryMB, maxMemoryMB
    ));
  }

  private boolean isValidLiftRide(LiftRideEvent liftRide) {
    if (liftRide == null) {
      return false;
    }
    return liftRide.getSkierId() > 0 && liftRide.getSkierId() <= ServerConfig.MAX_SKIER_ID &&
            liftRide.getResortId() > 0 && liftRide.getResortId() <= ServerConfig.MAX_RESORT_ID &&
            liftRide.getLiftId() > 0 && liftRide.getLiftId() <= ServerConfig.MAX_LIFT_ID &&
            liftRide.getSeasonId() == ServerConfig.SEASON_ID &&
            liftRide.getDayId() == ServerConfig.MAX_DAY_ID &&
            liftRide.getTime() > 0 && liftRide.getTime() <= ServerConfig.MAX_TIME;
  }

  private boolean isUrlValid(String urlPath) {
    // Expected format: /resorts/{resortId}/seasons/{seasonId}/days/{dayId}/skiers/{skierId}
    if (urlPath == null || urlPath.isEmpty()) {
      return false;
    }

    if (urlPath.startsWith("/")) {
      urlPath = urlPath.substring(1);
    }

    String[] urlParts = urlPath.split("/");

    if (urlParts.length != 8) {
      return false;
    }

    if (!urlParts[0].equals("resorts") ||
            !urlParts[2].equals("seasons") ||
            !urlParts[4].equals("days") ||
            !urlParts[6].equals("skiers")) {
      return false;
    }

    try {
      int resortId = Integer.parseInt(urlParts[1]);
      int seasonId = Integer.parseInt(urlParts[3]);
      int dayId = Integer.parseInt(urlParts[5]);
      int skierId = Integer.parseInt(urlParts[7]);

      return resortId > 0 && resortId <= ServerConfig.MAX_RESORT_ID &&
              seasonId == ServerConfig.SEASON_ID &&
              dayId == ServerConfig.MAX_DAY_ID &&
              skierId > 0 && skierId <= ServerConfig.MAX_SKIER_ID;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  /**
   * Class to hold batch message items
   */
  private static class BatchItem {
    final String id;
    final String messageBody;

    BatchItem(String id, String messageBody) {
      this.id = id;
      this.messageBody = messageBody;
    }
  }

  @Override
  public void destroy() {
    // Flush any remaining messages
    flushMessageBatch();

    // Shutdown thread pools
    if (executorService != null) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
      }
    }

    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
      try {
        if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduledExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduledExecutor.shutdownNow();
      }
    }

    // Close SQS client
    if (sqsClient != null) {
      sqsClient.shutdown();
      getServletContext().log("SQS client closed");
    }

    super.destroy();
  }
}