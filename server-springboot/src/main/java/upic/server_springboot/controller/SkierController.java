package upic.server_springboot;

import upic.server_springboot.model.LiftRideEvent;
import upic.server_springboot.model.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/skiers")
public class SkierController {

  @PostMapping
  public ResponseEntity<?> recordLiftRide(@RequestBody LiftRideEvent liftRide) {
    if (!isValidLiftRide(liftRide)) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(new ErrorResponse("Invalid lift ride data", 400));
    }
    return ResponseEntity.status(HttpStatus.CREATED)
        .body("Lift ride recorded successfully");
  }

  private boolean isValidLiftRide(LiftRideEvent liftRide) {
    return liftRide.getSkierId() > 0 && liftRide.getSkierId() <= 100000 &&
        liftRide.getResortId() > 0 && liftRide.getResortId() <= 10 &&
        liftRide.getLiftId() > 0 && liftRide.getLiftId() <= 40 &&
        liftRide.getSeasonId() == 2025 &&
        liftRide.getDayId() == 1 &&
        liftRide.getTime() > 0 && liftRide.getTime() <= 360;
  }
}