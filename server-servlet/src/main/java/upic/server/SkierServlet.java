package upic.server;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.gson.Gson;
import upic.server.model.LiftRideEvent;
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

    // 直接创建一个简单的JSON对象
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
    BufferedReader reader = req.getReader();
    LiftRideEvent liftRide = gson.fromJson(reader, LiftRideEvent.class);

    if (!isValidLiftRide(liftRide)){
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      resp.getWriter().write("Invalid lift ride data");
      return;
    }
    resp.setStatus(HttpServletResponse.SC_CREATED);
    resp.getWriter().write("Lift ride recoreded successfully");

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
}
