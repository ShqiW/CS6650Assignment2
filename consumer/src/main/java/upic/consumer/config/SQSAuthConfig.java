package upic.consumer.config;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;

/**
 * 消费者端SQS认证配置类 - 提供不使用IAM的SQS连接方式
 */
public class SQSAuthConfig {

    // 配置项
    private static final String SQS_QUEUE_URL = ConsumerConfig.SQS_QUEUE_URL;
    private static final String AWS_REGION = ConsumerConfig.AWS_REGION;

    // 选项1: 使用环境变量 (在服务器环境变量中设置)
    private static AmazonSQS createSQSClientFromEnvVars() {
        // 访问密钥从环境变量获取
        String accessKey = System.getenv("AWS_ACCESS_KEY");
        String secretKey = System.getenv("AWS_SECRET_KEY");

        if (accessKey == null || secretKey == null) {
            throw new RuntimeException("环境变量未设置: AWS_ACCESS_KEY 或 AWS_SECRET_KEY");
        }

        // 创建基本凭证而不是IAM角色
        return AmazonSQSClientBuilder.standard()
                .withRegion(AWS_REGION)
                .withCredentials(new AWSStaticCredentialsProvider(
                        new com.amazonaws.auth.BasicAWSCredentials(accessKey, secretKey)))
                .build();
    }

    // 选项2: 使用配置文件
    private static AmazonSQS createSQSClientFromConfigFile() {
        // 从配置文件中获取凭证 - 系统会自动查找 ~/.aws/credentials
        return AmazonSQSClientBuilder.standard()
                .withRegion(AWS_REGION)
                .build(); // 使用默认凭证提供者链
    }

    // 选项3: 使用STS临时令牌 (需要最小权限的STS访问)
    private static AmazonSQS createSQSClientWithSTS(String roleArn, String roleSessionName) {
        // 创建STS客户端 (仍需要最小权限来调用STS)
        AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
                .withRegion(AWS_REGION)
                .build();

        // 请求临时凭证
        AssumeRoleRequest roleRequest = new AssumeRoleRequest()
                .withRoleArn(roleArn)
                .withRoleSessionName(roleSessionName)
                .withDurationSeconds(3600); // 1小时

        AssumeRoleResult roleResponse = stsClient.assumeRole(roleRequest);
        Credentials credentials = roleResponse.getCredentials();

        // 创建使用临时凭证的SQS客户端
        BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
                credentials.getAccessKeyId(),
                credentials.getSecretAccessKey(),
                credentials.getSessionToken());

        return AmazonSQSClientBuilder.standard()
                .withRegion(AWS_REGION)
                .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
                .build();
    }

    // 选项4: 使用硬编码凭证 (不推荐用于生产环境，仅用于开发/测试)
    private static AmazonSQS createSQSClientWithHardcodedCredentials(String accessKey, String secretKey) {
        return AmazonSQSClientBuilder.standard()
                .withRegion(AWS_REGION)
                .withCredentials(new AWSStaticCredentialsProvider(
                        new com.amazonaws.auth.BasicAWSCredentials(accessKey, secretKey)))
                .build();
    }

    // 选项5: 使用EC2实例profile (如果运行在EC2上)
    private static AmazonSQS createSQSClientFromEC2InstanceProfile() {
        // EC2实例上的应用会自动使用实例profile
        return AmazonSQSClientBuilder.standard()
                .withRegion(AWS_REGION)
                .build();  // 不需要提供凭证，系统会自动查找EC2实例profile
    }

    /**
     * 获取SQS客户端 - 根据环境选择适当的认证方法
     */
    public static AmazonSQS getSQSClient() {
        // 从环境变量确定使用哪种认证方式
        String authMethod = System.getProperty("aws.authMethod", "env");

        switch(authMethod) {
            case "env":
                return createSQSClientFromEnvVars();
            case "file":
                return createSQSClientFromConfigFile();
            case "sts":
                String roleArn = System.getProperty("aws.roleArn");
                return createSQSClientWithSTS(roleArn, "SkiResortConsumerSession");
            case "ec2":
                return createSQSClientFromEC2InstanceProfile();
            case "direct":
                String accessKey = System.getProperty("aws.accessKey");
                String secretKey = System.getProperty("aws.secretKey");
                return createSQSClientWithHardcodedCredentials(accessKey, secretKey);
            default:
                throw new IllegalArgumentException("未知的认证方法: " + authMethod);
        }
    }

    /**
     * 获取队列URL
     */
    public static String getQueueUrl() {
        return SQS_QUEUE_URL;
    }
}