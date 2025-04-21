package org.example.MongoDB;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;


public class MongoDB {
    private final MongoClient mongoClient;
    private final String databaseName;
    private final MongoDatabase db;
    private final String[] externalOpLogPaths={
            "/home/subham05/Desktop/NoSQL/NoSQLProject/src/data/hive_oplog.csv",
            "/home/subham05/Desktop/NoSQL/NoSQLProject/src/data/postgre_oplog.csv"
    };


    public MongoDB(MongoClient mongoClient, String databaseName) {
        this.mongoClient = mongoClient;
        this.databaseName = databaseName;
        this.db = mongoClient.getDatabase(databaseName);
    }


    public MongoCollection<Document> getCollection(String collectionName) {
        return db.getCollection(collectionName);
    }

    OpLog opLog = new OpLog();

    public void readDataByField(String collectionName, String studentID, String courseID, String fieldName) {
        MongoCollection<Document> collection = getCollection(collectionName);
        Document query = new Document("student-ID", studentID)
                .append("course-id", courseID);

        Document doc = collection.find(query).first();

        if (doc == null) {
            System.out.println("No record found for Student ID: " + studentID + " and Course ID: " + courseID);
            return;
        }

        if (fieldName.equalsIgnoreCase("NA")) {
            // Print full document
            System.out.println(doc.toJson());
        } else {
            Object value = doc.get(fieldName);
            if (value != null) {
                System.out.println("Student ID: " + studentID + ", Course ID: " + courseID + ", " + fieldName + ": " + value);
                opLog.writeToOplog(collectionName, studentID, courseID, fieldName, "GET", "NA");
            } else {
                System.out.println("Field '" + fieldName + "' not found for Student ID: " + studentID + " and Course ID: " + courseID);
            }
        }
    }



    //update data by field
    public void updateDataByField(String collectionName, String studentID, String courseID, String fieldName, String newValue) {
        MongoCollection<Document> collection = getCollection(collectionName);
        Document query = new Document("student-ID", studentID)
                .append("course-id", courseID);
        Document update = new Document("$set", new Document(fieldName, newValue));

        collection.updateOne(query, update);
        System.out.println("Data updated successfully.");

        // Write to oplog using studentID as column for tracking
        opLog.writeToOplog(collectionName, studentID , courseID, fieldName, "SET", newValue);
    }





    //merge function
    public void mongoMerge(String source) {
        String oplogPath;
        if ("hive".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[0];
        } else if ("postgresql".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[1];
        } else {
            System.out.println("Invalid source: " + source);
            return;
        }

        // Map of latest ops from external and mongo oplogs
        Map<String, OplogEntry> externalOps = new HashMap<>();
        Map<String, OplogEntry> mongoOps = new HashMap<>();

        opLog.readOplog(oplogPath, externalOps);
        opLog.readOplog("/home/subham05/Desktop/NoSQL/NoSQLProject/src/data/mongo_oplog.csv", mongoOps);

        //print both maps
        System.out.println("External Oplog Entries:");
        for (OplogEntry entry : externalOps.values()) {
            System.out.println(entry.studentID + " | Course: " + entry.courseID
                    + " | Field: " + entry.column + " | Value: " + entry.newValue);
        }

        System.out.println("Mongo Oplog Entries:");
        for (OplogEntry entry : mongoOps.values()) {
            System.out.println(entry.studentID + " | Course: " + entry.courseID
                    + " | Field: " + entry.column + " | Value: " + entry.newValue);
        }

        for (String key : externalOps.keySet()) {
            OplogEntry externalEntry = externalOps.get(key);
            OplogEntry mongoEntry = mongoOps.get(key);

            boolean shouldUpdate = false;

            if (mongoEntry == null || externalEntry.timestamp.isAfter(mongoEntry.timestamp)) {
                shouldUpdate = true;
            }

            if (shouldUpdate) {
                updateDataByField(
                        externalEntry.tableName,
                        externalEntry.studentID,
                        externalEntry.courseID,
                        externalEntry.column,
                        externalEntry.newValue
                );

                System.out.println("Merged UPDATE to MongoDB for: "
                        + externalEntry.studentID + " | Course: " + externalEntry.courseID
                        + " | Field: " + externalEntry.column + " | Value: " + externalEntry.newValue);
            }
        }
        System.out.println("Merged MongoDB with " +source+  " successfully.");
    }



}
