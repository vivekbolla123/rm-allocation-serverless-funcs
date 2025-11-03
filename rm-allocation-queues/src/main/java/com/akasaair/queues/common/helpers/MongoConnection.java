package com.akasaair.queues.common.helpers;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ConnectionPoolSettings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Component
@Configuration
public class MongoConnection {

    @Value("${rm.db.host}")
    private String host;

    @Value("${rm.db.port}")
    private int port;

    @Value("${rm.db.username}")
    private String username;

    @Value("${RM_MONGO_PASSWORD}")
    private String password;

    @Value("${rm.db.dbname}")
    private String databaseName;

    @Value("${rm.db.auth-source}")
    private String authSource;

    // Singleton MongoClient instance (thread-safe with built-in connection pooling)
    private static volatile MongoClient mongoClient;
    public static volatile MongoDatabase database;

    // Private constructor to prevent instantiation
    public MongoConnection() {
    }

    /**
     * Get the singleton MongoClient instance with connection pooling
     * MongoClient is thread-safe and manages its own connection pool
     */
    public MongoClient getClient() {
        if (mongoClient == null) {
            synchronized (MongoConnection.class) {
                if (mongoClient == null) {
                    mongoClient = createMongoClient();
                }
            }
        }
        return mongoClient;
    }

    /**
     * Get the database instance
     * This is lightweight and can be called multiple times
     */
    public MongoDatabase getDatabase() {
        if (database == null) {
            synchronized (MongoConnection.class) {
                if (database == null) {
                    database = getClient().getDatabase(databaseName);
                }
            }
        }
        return database;
    }

    /**
     * Create MongoClient with custom connection pool settings
     */
    private MongoClient createMongoClient() {
        String encodedPassword = URLEncoder.encode(password, StandardCharsets.UTF_8);
        String mongoUri = String.format(
                "mongodb://%s:%s@%s:%d/?authSource=%s",
                username, encodedPassword, host, port, authSource
        );

        ConnectionString connectionString = new ConnectionString(mongoUri);

        // Configure connection pool settings
        ConnectionPoolSettings poolSettings = ConnectionPoolSettings.builder()
                .maxSize(100)                          // Maximum connections in pool
                .minSize(10)                           // Minimum connections in pool
                .maxWaitTime(2, TimeUnit.MINUTES)      // Max wait time for connection
                .maxConnectionIdleTime(10, TimeUnit.MINUTES)  // Idle connection timeout
                .maxConnectionLifeTime(30, TimeUnit.MINUTES)  // Max connection lifetime
                .build();

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .applyToConnectionPoolSettings(builder ->
                        builder.applySettings(poolSettings))
                .build();

        return MongoClients.create(settings);
    }

    /**
     * Gracefully close the MongoDB connection
     * Call this when your application shuts down
     */
    public static void close() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
            database = null;
        }
    }
}