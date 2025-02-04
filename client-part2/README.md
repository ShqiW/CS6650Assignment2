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
server.url=http://your-ec2-instance:8080/api/skiers
thread.count=32
request.count=200000
metrics.output.path=./metrics.csv
```
__2. Compile__
```java
mvn clean package
```
__3. run__
```java
java -jar target/client-part2.jar
```


## Configuration Details
### Key Settings
* server.url: Server API endpoint URL
* thread.count: Number of concurrent threads (default 32)
* request.count: Total request count (default 200000)
* metrics.output.path: Performance metrics CSV output path
* retry.max: Maximum retry attempts (default 5)


## Performance Metrics Output
### Console Output
```text
Mean Response Time: [mean_time] ms
Median Response Time: [median_time] ms
99th Percentile Response Time: [p99_time] ms
Min Response Time: [min_time] ms
Max Response Time: [max_time] ms
Throughput: [throughput] requests/second

```
### CSV File Format
```csv
startTime, requestType, latency, responseCode
```