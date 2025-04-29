package org.example.MongoDB;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.example.DatabaseDAOInterface;
import org.example.OpLog;
import org.example.OplogEntry;

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
        System.out.println("Data updated successfully.");

        opLog.writeToOplog(tableName, studentID, courseID, targetFieldName, "SET", newValue);
    }

    @Override
    public void Merge(String source) {
        String oplogPath;
        if ("hive".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[0];
        } else if ("postgresql".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[1];
        } else {
            System.out.println("Invalid source: " + source);
            return;
        }

        Map<String, OplogEntry> externalOps = new HashMap<>();
        Map<String, OplogEntry> mongoOps = new HashMap<>();

        opLog.readOplog(oplogPath, externalOps);
        opLog.readOplog("src/data/mongo_oplog.csv", mongoOps);

        System.out.println("External Oplog ("+ source +") Entries:");
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
                updateFieldByCompositeKey(
                        externalEntry.studentID,
                        externalEntry.courseID,
                        externalEntry.column,
                        externalEntry.newValue,
                        externalEntry.tableName
                );

                System.out.println("Merged UPDATE to MongoDB for: "
                        + externalEntry.studentID + " | Course: " + externalEntry.courseID
                        + " | Field: " + externalEntry.column + " | Value: " + externalEntry.newValue);
            }
        }
        System.out.println("Merged MongoDB with " + source + " successfully.");
    }

    public static void main(String[] args) throws Exception {
        DatabaseDAOInterface dao = new MongoDAO();
        String studentID = "SID1033";
        String courseID = "CSE016";
        String fieldName = "grade";
        String tableName = "student_course_grades";
        String fieldValue = dao.getFieldValueByCompositeKey(studentID, courseID, fieldName, tableName);
        System.out.println("Field Value: " + fieldValue);

        String newValue = "A";
        dao.updateFieldByCompositeKey(studentID, courseID, fieldName, newValue, tableName);
        String updatedFieldValue = dao.getFieldValueByCompositeKey(studentID, courseID, fieldName, tableName);
        System.out.println("Updated Field Value: " + updatedFieldValue);
        dao.Merge("hive");
    }
}