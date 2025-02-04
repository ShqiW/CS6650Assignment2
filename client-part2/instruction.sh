# 1. 同样需要确保ClientConfig.java中的BASE_URL配置正确

# 2. 在IntelliJ中打开client-part2项目
# 3. 运行主类SkiResortClientWithStats:
- 找到 SkiResortClientWithStats.java
- 右键点击
- 选择"Run 'SkiResortClientWithStats.main()'"

# 4. 观察控制台输出:
- 基础统计信息（与Part 1相同）
- 详细性能统计
  * 平均响应时间
  * 中位数响应时间
  * P99响应时间
  * 最小/最大响应时间
  * 吞吐量

# 5. 检查生成的CSV文件:
- 在项目根目录找到 request_statistics.csv
- 文件包含所有请求的详细信息

Client Part 2 Results:
Total Threads: 32
Wall Time: 177863 ms
Successful Requests: 200000
Failed Requests: 0

Detailed Performance Statistics:
Mean Response Time: 26.49 ms
Median Response Time: 25.00 ms
99th Percentile Response Time: 51.00 ms
Min Response Time: 12.00 ms
Max Response Time: 7463.00 ms
Throughput: 1124.46 requests/second


Little's Law states: N = X * R
where N = number of concurrent requests
X = throughput
R = response time

Using your single thread test:

R = 24.70ms = 0.0247 seconds
Theoretical max throughput with 32 threads:

X = 32 / 0.0247 = 1295 requests/second