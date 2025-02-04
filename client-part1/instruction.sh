# 1. 修改ClientConfig.java中的BASE_URL为你的EC2地址:
public static final String BASE_URL = "http://35.91.119.93:8080/server-1.0-SNAPSHOT/skiers";


# 2. 在IntelliJ中打开client-part1项目
# 3. 运行主类SkiResortClient:
- 找到 SkiResortClient.java
- 右键点击
- 选择"Run 'SkiResortClient.main()'"

# 4. 观察控制台输出:
- 总运行时间
- 成功/失败请求数
- 吞吐量

Starting single thread test ...

Benchmark Results:
Total Request: 10000
Successful Requests: 10000
Failed Requests: 0
Wall Time: 246977ms
Single Thread Throughput: 40.00 requests/second
Average Latency: 24.70 ms/request

Client Part 1 Results:
Total Threads: 32
Successful Requests: 200000
Failed Requests: 0
Wall Time: 227974 ms
Throughput: 877.29 requests/second

在单线程中，每个请求都必须等待前一个请求完成才能发送。也就是说：

一个请求需要24.70ms
所以1秒最多可以处理 1000ms/24.70ms ≈ 40个请求

在32线程的情况下：

32个请求可以同时发送
每个线程不需要等待其他线程的响应
理论上的最大吞吐量应该是：

单线程吞吐量 × 线程数
40 × 32 = 1280请求/秒

你实际获得了877请求/秒，这个数字比理论最大值低是正常的，原因包括：

线程调度开销
CPU资源竞争
网络带宽限制
服务器端的处理能力限制

877 / (40 × 32) ≈ 68.5%
这是一个相当不错的扩展效率，因为在实际应用中，很难达到100%的线性扩展。