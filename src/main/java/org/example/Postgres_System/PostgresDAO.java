package org.example.Postgres_System;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import org.example.DatabaseDAOInterface;
import org.example.OplogEntry;
import org.example.OpLog;

public class PostgresDAO implements DatabaseDAOInterface{

    private static final String URL = "jdbc:postgresql://localhost:5432/postgres_db";
    private static final String USER = "postgres";
    private static final String PASSWORD = "root";
    private static final String[] externalOpLogPaths={
            "src/data/mongo_oplog.csv",
            "src/data/hive_oplog.csv"
    };

    static OpLog opLog = new OpLog("src/data/postgres_oplog.csv")  ;

    // 1. Get field value by composite key
    @Override
    public String getFieldValueByCompositeKey(String studentID, String courseID, String fieldName, String tableName) throws SQLException {
        // Query to fetch the field value using the composite key (student-ID, course-ID)
        String query = String.format("SELECT \"%s\" FROM %s WHERE \"student-ID\" = ? AND \"course-id\" = ?", fieldName, tableName);
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, studentID);
            pstmt.setString(2, courseID);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    // Log the operation to the oplog
                    //OplogEntry entry = new OplogEntry(tableName, studentID, courseID, fieldName, rs.getString(fieldName), ZonedDateTime.now(), "GET");
                    //Oplog_PostgresDAO.insertOplogEntry(entry);
                    opLog.writeToOplog(tableName, studentID, courseID, fieldName, "GET", "NA");
                    return rs.getString(fieldName);
                } else {
                    throw new SQLException(String.format("No record found for Student ID '%s' and Course ID '%s'.", studentID, courseID));
                }
            }
        }
    }


    // 2. Update field value by composite key
    @Override
    public void updateFieldByCompositeKey(String studentID, String courseID, String targetFieldName, String newValue, String tableName) throws SQLException {
        // Dynamically include the table name and target field name in the query
        String update = String.format("UPDATE %s SET \"%s\" = ? WHERE \"student-ID\" = ? AND \"course-id\" = ?", tableName, targetFieldName);
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(update)) {
            pstmt.setString(1, newValue);
            pstmt.setString(2, studentID);
            pstmt.setString(3, courseID);
            int rows = pstmt.executeUpdate();
            if (rows == 0) {
                throw new SQLException(String.format("Update failed. No record found for Student ID '%s' and Course ID '%s'.", studentID, courseID));
            } else {
                // Log the operation to the oplog
                //OplogEntry entry = new OplogEntry(tableName, studentID, courseID, targetFieldName, newValue, ZonedDateTime.now(), "UPDATE");
                //Oplog_PostgresDAO.insertOplogEntry(entry);
                opLog.writeToOplog(tableName, studentID, courseID, targetFieldName, "SET", newValue);
                System.out.println("Updated successfully.");
            }
        }
    }
    @Override
    public void Merge(String source) throws Exception {
        String oplogPath;
        if ("mongo".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[0];
        } else if ("hive".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[1];
        } else {
            System.out.println("Invalid source: " + source);
            return;
        }

        DatabaseDAOInterface dao = new org.example.Postgres_System.PostgresDAO();
        // Map of latest ops from external and mongo oplogs
        Map<String, OplogEntry> externalOps = new HashMap<>();
        Map<String, OplogEntry> postgresOps = new HashMap<>();

        opLog.readOplog(oplogPath, externalOps);
        opLog.readOplog("src/data/postgres_oplog.csv", postgresOps);

        //print both maps
        System.out.println("External Oplog Entries:");
        for (OplogEntry entry : externalOps.values()) {
            System.out.println(entry.studentID + " | Course: " + entry.courseID
                    + " | Field: " + entry.column + " | Value: " + entry.newValue);
        }

        System.out.println("Postgres Oplog Entries:");
        for (OplogEntry entry : postgresOps.values()) {
            System.out.println(entry.studentID + " | Course: " + entry.courseID
                    + " | Field: " + entry.column + " | Value: " + entry.newValue);
        }

        for (String key : externalOps.keySet()) {
            OplogEntry externalEntry = externalOps.get(key);
            OplogEntry postgresEntry = postgresOps.get(key);

            boolean shouldUpdate = false;

            if (postgresEntry == null || externalEntry.timestamp.isAfter(postgresEntry.timestamp)) {
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

                System.out.println("Merged UPDATE to Postgres for: "
                        + externalEntry.studentID + " | Course: " + externalEntry.courseID
                        + " | Field: " + externalEntry.column + " | Value: " + externalEntry.newValue);
            }
        }
        System.out.println("Merged Postgres with " +source+  " successfully.");
    }

    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");
            DatabaseDAOInterface dao = new PostgresDAO();
            //String studentID = "SID1033";
            //String courseID = "CSE016";
            //String fieldName = "grade";
            //String tableName = "student_course_grades";
            //String fieldValue = dao.getFieldValueByCompositeKey(studentID, courseID, fieldName, tableName);
            //System.out.println("Field Value: " + fieldValue);

            //String newValue = "A";
            //dao.updateFieldByCompositeKey(studentID, courseID, fieldName, newValue, tableName);
            //String updatedFieldValue = dao.getFieldValueByCompositeKey(studentID, courseID, fieldName, tableName);
            //System.out.println("Updated Field Value: " + updatedFieldValue);
            //postgresMerge("mongo");
            dao.Merge("hive");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}