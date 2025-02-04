package cs6650.server;

import com.google.gson.Gson;
import cs6650.server.model.LiftRideEvent;
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

//  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
//    res.setContentType("text/plain");
//    String urlPath = req.getPathInfo();
//
//    if (urlPath == null || urlPath.isEmpty()) {
//      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
//      res.getWriter().write("missing paramterers");
//      return;
//    }
//
//    String[] urlParts = urlPath.split("/");
//
//    if (!isUrlValid(urlParts)) {
//      res.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
//      res.getWriter().write("invalid url path");
//    }
//    res.setStatus(HttpServletResponse.SC_OK);
//    String jsonResponse = "{\"status\":200}";
//    res.getWriter().write(jsonResponse);
//
//  }


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
