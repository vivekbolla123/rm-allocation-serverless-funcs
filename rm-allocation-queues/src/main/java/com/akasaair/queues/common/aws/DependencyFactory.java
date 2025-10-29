
package com.akasaair.queues.common.aws;

import com.akasaair.queues.App;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.ssm.SsmClient;

/**
 * The module containing all dependencies required by the {@link App}.
 */
@Configuration
public class DependencyFactory {

    public DependencyFactory() {}

    @Value("${RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD}")
    private String accessKey;

    @Value("${RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD}")
    private String secretKey;

    public S3Client s3Client() {
        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.AP_SOUTH_1)
                .build();
    }

    public SsmClient ssmClient() {
        return SsmClient.builder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .region(Region.AP_SOUTH_1)
                .build();
    }

}
