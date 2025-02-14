package upic.server_springboot.model;
import lombok.Data;


@Data
public class LiftRideEvent{
  private int skierId;
  private int resortId;
  private int liftId;
  private int seasonId;
  private int dayId;
  private int time;

}