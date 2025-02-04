# 1. 在IntelliJ中打开server项目
# 2. 确保配置了Tomcat:
- 点击右上角的"Edit Configurations"
- 点击"+"添加新配置
- 选择"Tomcat Server" -> "Local"
- 在"Deployment"标签页，点击"+"
- 选择"Artifact" -> "skier-server:war"
- 设置应用程序上下文(Application context)为 "/skier-server"

# 3. 点击运行按钮在本地启动服务器测试
# 4. 用Postman测试API是否工作:
POST http://localhost:8080/skier-server/skiers
Content-Type: application/json

{
    "skierId": 123,
    "resortId": 5,
    "liftId": 15,
    "seasonId": 2025,
    "dayId": 1,
    "time": 217
}

# 5. 确认工作后，生成WAR文件:
- 右键点击项目
- 选择"Build Artifact"
- 选择"skier-server:war"
- 在target文件夹找到生成的WAR文件

# 6. 将WAR文件部署到EC2上的Tomcat
# 连接到EC2
ssh -i /Users/wsq/Documents/CS_Align/CS6650/CS6650.pem ec2-user@35.91.119.93

# 在本地电脑执行
scp -i ~/Documents/CS_Align/CS6650/CS6650.pem target/server-1.0-SNAPSHOT.war ec2-user@35.91.119.93:/usr/share/tomcat/webapps/

# 在EC2上查看日志
cd /opt/tomcat/logs
tail -f catalina.out


# 在本地测试API
curl -X POST \
  http://your-ec2-ip:8080/server/skiers \
  -H 'Content-Type: application/json' \
  -d '{
    "skierId": 123,
    "resortId": 5,
    "liftId": 15,
    "seasonId": 2025,
    "dayId": 1,
    "time": 217
}'
