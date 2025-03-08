package upic.server;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.google.gson.Gson;
import upic.server.model.ErrorResponse;
import upic.server.model.LiftRideEvent;
import upic.server.model.SuccessResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/skiers/*")
public class SkierServlet extends HttpServlet {
  private final Gson gson = new Gson();
  private AmazonSQS sqsClients;
  private String queueUrl;


  @Override
  public void init() throws ServletException {
    super.init();

    try{
      String accessKey = System.getProperty("aws.accessKeyId");
      String accessKeySecret = System.getProperty("aws.secretkey");
      String region = System.getProperty("aws.region","us-west-2");
      queueUrl = System.getProperty("aws.queueUrl");

      if (accessKey == null || accessKeySecret == null || region == null) {
        getServletContext().log("Access Key/Secret or Region not specified");
        sqsClients = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.fromName(region))
                .build();
      }else {
        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, accessKeySecret);
        sqsClients = AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.fromName(region))
                .build();
      }
      getServletContext().log("SQS client created. Here is the queue URl: " + queueUrl);



    } catch (Exception e) {
      getServletContext().log("SQS client creation failed " + e.getMessage());
      throw new ServletException("cannot initialize SQS client", e);

    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res)
      throws IOException {
    res.setContentType("application/json");
    PrintWriter out = res.getWriter();

    // create json object
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
    resp.setContentType("application/json");
    PrintWriter writer = resp.getWriter();

    // Direct console output for debugging
    System.out.println("********** DEBUG START **********");
    System.out.println("Request URL: " + req.getRequestURL());

    // 1. Validate URL path
    String urlPath = req.getPathInfo();
    System.out.println("Path Info: " + urlPath);

    // Print URL segments
    if (urlPath != null) {
      if (urlPath.startsWith("/")) {
        String[] parts = urlPath.substring(1).split("/");
        System.out.println("URL segments count: " + parts.length);
        for (int i = 0; i < parts.length; i++) {
          System.out.println("Segment[" + i + "]: " + parts[i]);
        }
      }
    }

    // Check URL validation
    boolean isValid = isUrlValid(urlPath);
    System.out.println("URL validation result: " + isValid);
    System.out.println("********** DEBUG END **********");

    if (!isValid) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ErrorResponse error = new ErrorResponse("Invalid URL path", HttpServletResponse.SC_BAD_REQUEST);
      writer.write(gson.toJson(error));
      return;
    }

    try {
      // 2. Parse and validate JSON payload
      BufferedReader reader = req.getReader();
      LiftRideEvent liftRide = gson.fromJson(reader, LiftRideEvent.class);

      if (!isValidLiftRide(liftRide)) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        ErrorResponse error = new ErrorResponse("Invalid lift ride data", HttpServletResponse.SC_BAD_REQUEST);
        writer.write(gson.toJson(error));
        return;
      }

      // 3. Extract additional information from URL if needed
      // For example, you might need to extract resortId, seasonId, dayId, skierId from URL
      // and ensure they match the values in the JSON payload

      // 4. Send message to SQS queue
      try {
        String messageBody = gson.toJson(liftRide);
        SendMessageRequest sendMsgRequest = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(messageBody);

        SendMessageResult result = sqsClients.sendMessage(sendMsgRequest);

        // 5. Return success response
        resp.setStatus(HttpServletResponse.SC_CREATED);
        SuccessResponse success = new SuccessResponse("Lift ride recorded successfully, message ID: " + result.getMessageId());
        writer.write(gson.toJson(success));

        System.out.println("Message sent to SQS successfully, message ID: " + result.getMessageId());
      } catch (Exception e) {
        System.out.println("Error sending message to SQS: " + e.getMessage());
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        ErrorResponse error = new ErrorResponse("Error sending to queue: " + e.getMessage(),
                HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        writer.write(gson.toJson(error));
      }
    } catch (Exception e) {
      // Handle JSON parsing or other exceptions
      System.out.println("Error processing request: " + e.getMessage());
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      ErrorResponse error = new ErrorResponse("Invalid request format: " + e.getMessage(),
              HttpServletResponse.SC_BAD_REQUEST);
      writer.write(gson.toJson(error));
    }
  }



  private boolean isValidLiftRide(LiftRideEvent liftRide){
    if (liftRide == null){return false;}
    return liftRide.getSkierId() > 0 && liftRide.getSkierId()<=100000 &&
        liftRide.getResortId() > 0 && liftRide.getResortId() <= 10 &&
        liftRide.getLiftId()> 0 && liftRide.getLiftId() <= 40 &&
        liftRide.getSeasonId() == 2025 &&
        liftRide.getDayId() == 1 &&
        liftRide.getTime() > 0 && liftRide.getTime() <= 360;
  }

  private boolean isUrlValid(String urlPath) {
    // expected format: /resorts/{resortId}/seasons/{seasonId}/days/{dayId}/skiers/{skierId}
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


      return resortId > 0 && resortId <= 10 &&
              seasonId == 2025 &&
              dayId == 1 &&
              skierId > 0 && skierId <= 100000;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  @Override
  public void destroy() {
    // close SQS client
    if (sqsClients != null) {
      sqsClients.shutdown();
      getServletContext().log("SQS客 client closed");
    }
    super.destroy();
  }
}

