# Client 2 : Advanced Performance Metrics

## Project Description
Part 2 of the Ski Resort Management System client implementation, adding detailed performance metrics to Part 1.

## New Features
* Detailed request latency statistics
* Response time distribution analysis
* CSV performance logging
* P99 performance metrics calculation

## Quick Start
### Requirements
* Java 17
* Maven 3.6+

### Setup Steps
__1. Modify Configuration__
```java
BASE_URL = "http://35.91.119.93:8080/server-1.0-SNAPSHOT/skiers" // for server-servlet
BASE_URL = "http://35.91.119.93:8081/skiers" // for server-springboot
```
__2. Compile__
```java
mvn clean package
```
__3. run__
```java
java -jar target/client-part2-1.0-SNAPSHOT-jar-with-dependencies.jar
```


## Performance Metrics Output
### Console Output
```text
Starting client...
Configuration:
 - Initial Threads: 32
Initial phase completed
 - Remaining Requests: 168000
 - Additional Threads Used: 32
Remaining phase completed

Client Part 2 Results:
Total Threads: 32
Additional Threads Used: 32
Successful Requests: 200000
Failed Requests: 0
Wall Time: 234947 ms

Detailed Performance Statistics:
Mean Response Time: 34.96 ms
Median Response Time: 26.00 ms
99th Percentile Response Time: 127.00 ms
Min Response Time: 13.00 ms
Max Response Time: 4723.00 ms
Throughput: 851.26 requests/second


```
### CSV File Format
```csv
startTime, requestType, latency, responseCode
```
