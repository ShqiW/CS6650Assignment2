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
```bash
server.url=http://35.91.119.93:8080/server-1.0-SNAPSHOT/skiers
thread.count=32
request.count=200000
```

__2. Compile__
```bash
mvn clean package
```

__3. run__
```bash
java -jar target/client-part1-1.0-SNAPSHOT.jar

or

java -jar target/client-part1-1.0-SNAPSHOT-jar-with-dependencies.jar
```



## Configuration Details
### Key Settings
* server.url: Server API endpoint URL
* thread.count: Number of concurrent threads (default 32)
* request.count: Total request count (default 200000)
* retry.max: Maximum retry attempts (default 5)
* queue.size: Event queue size (default 1000)

## Output Format
```text
Total Threads: [thread_count]
Successful Requests: [success_count]
Failed Requests: [fail_count]
Wall Time: [total_time] ms
Throughput: [throughput] requests/second
```
