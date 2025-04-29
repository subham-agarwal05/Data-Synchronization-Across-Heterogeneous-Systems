package org.example.MongoDB;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.example.DatabaseDAOInterface;
import org.example.OpLog;
import org.example.OplogEntry;
import java.io.*;

import java.util.HashMap;
import java.util.Map;

public class MongoDAO implements DatabaseDAOInterface {
    private static final String url = "mongodb://localhost:27017";
    private static final MongoClient mongoClient = MongoClients.create(url);
    private static final String databaseName = "nosql";
    private final MongoDatabase db = mongoClient.getDatabase(databaseName);
    private final String[] externalOpLogPaths = {
            "src/data/hive_oplog.csv",
            "src/data/postgres_oplog.csv"
    };

    private final OpLog opLog = new OpLog("src/data/mongo_oplog.csv");


    private MongoCollection<Document> getCollection(String collectionName) {
        return db.getCollection(collectionName);
    }

    @Override
    public String getFieldValueByCompositeKey(String studentID, String courseID, String fieldName, String tableName) {
        MongoCollection<Document> collection = getCollection(tableName);
        Document query = new Document("student-ID", studentID)
                .append("course-id", courseID);

        Document doc = collection.find(query).first();

        if (doc == null) {
            System.out.println("No record found for Student ID: " + studentID + " and Course ID: " + courseID);
            return null;
        }

        if (fieldName.equalsIgnoreCase("NA")) {
            System.out.println(doc.toJson());
            return null;
        } else {
            Object value = doc.get(fieldName);
            if (value != null) {
                System.out.println("Student ID: " + studentID + ", Course ID: " + courseID + ", " + fieldName + ": " + value);
                opLog.writeToOplog(tableName, studentID, courseID, fieldName, "GET", "NA");
                return value.toString();
            } else {
                System.out.println("Field '" + fieldName + "' not found for Student ID: " + studentID + " and Course ID: " + courseID);
                return null;
            }
        }
    }

    @Override
    public void updateFieldByCompositeKey(String studentID, String courseID, String targetFieldName, String newValue, String tableName) {
        MongoCollection<Document> collection = getCollection(tableName);
        Document query = new Document("student-ID", studentID)
                .append("course-id", courseID);
        Document update = new Document("$set", new Document(targetFieldName, newValue));

        collection.updateOne(query, update);

        opLog.writeToOplog(tableName, studentID, courseID, targetFieldName, "SET", newValue);
    }

    @Override
    public void Merge(String source) throws Exception {
        String oplogPath;
        String target = "mongo"; // Target database is now MongoDB

        if ("hive".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[1];
        } else if ("sql".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[0];
        } else {
            System.out.println("Invalid source: " + source);
            return;
        }

        // File to store the last processed OpIDs for the specific source-target pair
        String opIdStateFile = "src/data/" + target.toLowerCase() + "_" + source.toLowerCase() + "_opid_state.txt";
        int lastProcessedMongoOpId = 0;
        int lastProcessedExternalOpId = 0;

        // Read the last processed OpIDs from the file
        File file = new File(opIdStateFile);
        if (file.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String[] opIds = reader.readLine().split(",");
                lastProcessedMongoOpId = Integer.parseInt(opIds[0]);
                lastProcessedExternalOpId = Integer.parseInt(opIds[1]);
            } catch (IOException | NumberFormatException e) {
                System.err.println("Error reading OpID state file: " + e.getMessage());
            }
        } else {
            // Create the file and initialize it with "0,0"
            try (FileWriter writer = new FileWriter(file)) {
                writer.write("0,0");
                System.out.println("OpID state file created and initialized with 0,0: " + opIdStateFile);
            } catch (IOException e) {
                System.err.println("Error creating OpID state file: " + e.getMessage());
            }
        }

        DatabaseDAOInterface dao = new org.example.MongoDB.MongoDAO();
        Map<String, OplogEntry> externalOps = new HashMap<>();
        Map<String, OplogEntry> mongoOps = new HashMap<>();

        // Read oplogs and filter based on last processed OpIDs
        lastProcessedExternalOpId = opLog.readOplog(oplogPath, externalOps, lastProcessedExternalOpId);
        lastProcessedMongoOpId = opLog.readOplog("src/data/mongo_oplog.csv", mongoOps, lastProcessedMongoOpId);

//        // Print filtered oplogs
//        System.out.println("Filtered External Oplog (" + source + ") Entries:");
//        for (OplogEntry entry : externalOps.values()) {
//            System.out.println(entry.studentID + " | Course: " + entry.courseID
//                    + " | Field: " + entry.column + " | Value: " + entry.newValue);
//        }
//        System.out.println();
//
//        System.out.println("Filtered MongoDB Oplog Entries:");
//        for (OplogEntry entry : mongoOps.values()) {
//            System.out.println(entry.studentID + " | Course: " + entry.courseID
//                    + " | Field: " + entry.column + " | Value: " + entry.newValue);
//        }
//        System.out.println();

        // Merge logic
        int maxExternalOpId = lastProcessedExternalOpId;
        int maxMongoOpId = lastProcessedMongoOpId;

        for (String key : externalOps.keySet()) {
            OplogEntry externalEntry = externalOps.get(key);
            OplogEntry mongoEntry = mongoOps.get(key);

            boolean shouldUpdate = false;

            if (mongoEntry == null || externalEntry.timestamp.isAfter(mongoEntry.timestamp)) {
                shouldUpdate = true;
            }

            if (shouldUpdate) {
                dao.updateFieldByCompositeKey(
                        externalEntry.studentID,
                        externalEntry.courseID,
                        externalEntry.column,
                        externalEntry.newValue,
                        externalEntry.tableName
                );
                lastProcessedMongoOpId+=1;
                System.out.println("Merged UPDATE to MongoDB for: "
                        + externalEntry.studentID + " | Course: " + externalEntry.courseID
                        + " | Field: " + externalEntry.column + " | Value: " + externalEntry.newValue);
            }
        }

        // Update the OpID state file
        try (FileWriter writer = new FileWriter(opIdStateFile)) {
            writer.write(maxMongoOpId + "," + maxExternalOpId);
        } catch (IOException e) {
            System.err.println("Error updating OpID state file: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        DatabaseDAOInterface dao = new MongoDAO();
//        String studentID = "SID1033";
//        String courseID = "CSE016";
//        String fieldName = "grade";
//        String tableName = "student_course_grades";
//        String fieldValue = dao.getFieldValueByCompositeKey(studentID, courseID, fieldName, tableName);
//        System.out.println("Field Value: " + fieldValue);
//
//        String newValue = "A";
//        dao.updateFieldByCompositeKey(studentID, courseID, fieldName, newValue, tableName);
//        String updatedFieldValue = dao.getFieldValueByCompositeKey(studentID, courseID, fieldName, tableName);
//        System.out.println("Updated Field Value: " + updatedFieldValue);
//        dao.Merge("hive");
        dao.Merge("sql");
        dao.Merge("mongo");
    }
}