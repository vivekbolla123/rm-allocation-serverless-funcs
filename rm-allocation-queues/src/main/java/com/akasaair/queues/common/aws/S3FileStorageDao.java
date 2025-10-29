package com.akasaair.queues.common.aws;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.akasaair.queues.common.constants.Constants.*;

@Component
public class S3FileStorageDao {
    private final S3Client s3Client;

    @Value("${rm.file-path}")
    private String S3_FILE_PATH_NAME;

    @Value("${rm.s3-bucket}")
    private String S3_BUCKET_NAME;

    public S3FileStorageDao(DependencyFactory dependencyFactory) {
        s3Client = dependencyFactory.s3Client();
    }

    public void storeFile(ByteArrayInputStream file, String tableName, String fileName) throws IOException {
        byte[] buffer = file.readAllBytes();
        String filePath = generatePath(tableName, fileName);

        // Simple, clean, does the same thing
        PutObjectResponse response = s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(S3_BUCKET_NAME)
                        .key(filePath)
                        .build(),
                RequestBody.fromBytes(buffer) // Note: RequestBody, not AsyncRequestBody
        );

        if (response.eTag() == null || response.eTag().isEmpty()) {
            throw new RuntimeException("Failed to upload file to S3");
        }

        file.close();
    }

    private String generatePath(String path, String fileName) {
        // Example - test/rm-admin/d1_d2_strategies/3b908569-bc73-4745-9bcd-2c1453e218cc_d1_d2_strategies
        return S3_FILE_PATH_NAME + path + SLASH + fileName + UNDERSCORE + path;
    }
}
