package com.akasaair.queues.common.aws;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.akasaair.queues.common.constants.Constants.*;

public class S3FileStorageDao {
    private final S3AsyncClient s3Client;

    public S3FileStorageDao() {
        s3Client = DependencyFactory.s3Client();
    }

    public void storeFile(ByteArrayInputStream file, String tableName, String fileName) throws IOException, ExecutionException, InterruptedException {
//        byte[] buffer = new byte[file.available()]; // Read the data into the buffer

        byte[] buffer = file.readAllBytes();
        String filePath = generatePath(tableName, fileName);

        CompletableFuture<PutObjectResponse> future = s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(S3_BUCKET_NAME)
                        .key(filePath)
                        .build(),
                AsyncRequestBody.fromBytes(buffer)
        );

        // Use future to handle the response or block for the response
        PutObjectResponse putObjectResponse = future.get(); // This will block until the operation is complete

        // Check if the ETag is not blank to confirm the upload was successful
        if (putObjectResponse.eTag() == null || putObjectResponse.eTag().isEmpty()) {
            throw new RuntimeException("Failed to upload file to S3");
        }

        file.close();
    }

    private String generatePath(String path, String fileName) {
        // Example - test/rm-admin/d1_d2_strategies/3b908569-bc73-4745-9bcd-2c1453e218cc_d1_d2_strategies
        return S3_FILE_PATH_NAME + path + SLASH + fileName + UNDERSCORE + path;
    }
}
