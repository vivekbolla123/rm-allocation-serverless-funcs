package com.akasaair.queues.common.helpers;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ConnectionPoolSettings;

import java.util.concurrent.TimeUnit;

public class MongoConnection {

    // MongoDB connection string from environment variables
    private static final String MONGO_URI = System.getenv("RM_MONGO_URI");
    // Example format: mongodb://username:password@host:port/database
    // or mongodb+srv://username:password@cluster.mongodb.net/database

    private static final String DATABASE_NAME = System.getenv("RM_DB_NAME");

    // Singleton MongoClient instance (thread-safe with built-in connection pooling)
    private static volatile MongoClient mongoClient;
 public static volatile MongoDatabase database;

    // Private constructor to prevent instantiation
    private MongoConnection() {
    }

    /**
     * Get the singleton MongoClient instance with connection pooling
     * MongoClient is thread-safe and manages its own connection pool
     */
    public static MongoClient getClient() {
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
    public static MongoDatabase getDatabase() {
        if (database == null) {
            synchronized (MongoConnection.class) {
                if (database == null) {
                    database = getClient().getDatabase(DATABASE_NAME);
                }
            }
        }
        return database;
    }

    /**
     * Create MongoClient with custom connection pool settings
     */
    private static MongoClient createMongoClient() {
        ConnectionString connectionString = new ConnectionString(MONGO_URI);

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
