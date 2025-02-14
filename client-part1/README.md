# Client Part 1 - Basic Implementation

## Project Description
Part 1 of the Ski Resort Management System client implementation, focusing on efficient bulk POST requests.

## Features
* Multi-threaded concurrent requests
* Automatic random ski record generation
* Error retry mechanism
* Basic performance statistics

## Quick Start
### Requirements
* Java 17
* Maven 3.6+

### Setup Steps
__1. Modify configuration__
Edit in client/config/ClientConfig.java
```bash
BASE_URL = "http://35.91.119.93:8080/server-1.0-SNAPSHOT/skiers" // for server-servlet
BASE_URL = "http://35.91.119.93:8081/skiers" // for server-springboot
```

__2. Compile__
```bash
mvn clean package
```

__3. run__
```bash
java -jar target/client-part1-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Output Format
```text
upic/client/SkiResortClient.class
Starting single thread benchmark...

Benchmark Results:
Total Requests: 10000
Successful Requests: 10000
Failed Requests: 0
Wall Time: 316381ms
Throughput: 31.00 requests/second
Average Latency: 31.64 ms/request
Starting client...
Configuration:
 - Initial Threads: 32
Initial phase completed
 - Remaining Requests: 168000
 - Additional Threads Used: 32
Remaining phase completed

Client Results:
Total Requests: 200000
Successful Requests: 200000
Failed Requests: 0
Wall Time: 223274 ms
Throughput: 895.76 requests/second

```
