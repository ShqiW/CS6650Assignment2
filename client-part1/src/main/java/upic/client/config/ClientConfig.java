package upic.client.config;

public class ClientConfig {
//  public static final String BASE_URL = "http://35.91.119.93:8080/server-1.0-SNAPSHOT/skiers"; // server-servlet
    public static final String BASE_URL = "http://35.91.119.93:8081/skiers"; // server-Springboot
  public static final int TOTAL_REQUESTS = 200000;
  public static final int INITIAL_THREADS = 32;
  public static final int REQUESTS_PER_THREAD = 1000;
  public static final int MAX_RETRY_ATTEMPTS = 5;
  public static final int QUEUE_SIZE = 10000;

  public String getBaseUrl(){return BASE_URL;}
  public int getTotalRequests(){return TOTAL_REQUESTS;}

  public int getInitialThreads(){return INITIAL_THREADS;}

  public  int getRequestsPerThread(){return REQUESTS_PER_THREAD;}
  public int getMaxRetryAttempts(){return MAX_RETRY_ATTEMPTS;}
  public int getQueueSize(){return QUEUE_SIZE;}
}