# Ski Resort Server

## Project Description
Server-side implementation of the Ski Resort Management System, handling lift ride records.

## Tech Stack
* Java Servlet
* Tomcat 9
* WAR deployment

## Quick Start
### Requirement
* Java 17
* Tomcat 9.x
* AWS EC2 instance

### Deployment Steps
1. Upload WAR file to EC2
```bash
scp -i your-key.pem server.war ec2-user@your-ec2-instance:/path/to/tomcat/webapps/
```
2. Start Tomcat
```bash
sudo systemctl start tomcat
```
3. Check deployment status
```bash
sudo systemctl status tomcat
```

### API Endpoint
Request Body:
```json
{
  "skierID": 123,      // 1-100000
  "resortID": 1,       // 1-10
  "liftID": 2,         // 1-40
  "seasonID": 2025,    // fixed as 2025
  "dayID": 1,          // fixed as 1
  "time": 300          // 1-360
}
```

Response:
* 201: Created - Record created successfully
* 400: Bad Request - Invalid parameters
* 500: Internal Server Error - Server error

### Performance Metrics
* Average response time < 50ms
* Handles 200K+ requests