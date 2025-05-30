package org.example.Postgres_System;

import java.io.*;
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
            }
        }
    }
    @Override
    public void Merge(String source) throws Exception {
        String oplogPath;
        String target = "postgres"; // Target database is always Postgres in this case

        if ("mongo".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[0];
        } else if ("hive".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[1];
        } else {
            System.out.println("Invalid source: " + source);
            return;
        }

        // File to store the last processed OpIDs for the specific source-target pair
        // File to store the last processed OpIDs for the specific source-target pair
        String opIdStateFile = "src/data/" + target.toLowerCase() + "_" + source.toLowerCase() + "_opid_state.txt";
        int lastProcessedPostgresOpId = 0;
        int lastProcessedExternalOpId = 0;

        // Read the last processed OpIDs from the file
        File file = new File(opIdStateFile);
        if (file.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String[] opIds = reader.readLine().split(",");
                lastProcessedPostgresOpId = Integer.parseInt(opIds[0]);
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

        DatabaseDAOInterface dao = new org.example.Postgres_System.PostgresDAO();
        Map<String, OplogEntry> externalOps = new HashMap<>();
        Map<String, OplogEntry> postgresOps = new HashMap<>();

        // Read oplogs and filter based on last processed OpIDs
        lastProcessedExternalOpId = opLog.readOplog(oplogPath, externalOps, lastProcessedExternalOpId);
        lastProcessedPostgresOpId = opLog.readOplog("src/data/postgres_oplog.csv", postgresOps, lastProcessedPostgresOpId);

//        // Print filtered oplogs
//        System.out.println("Filtered External Oplog (" + source + ") Entries:");
//        for (OplogEntry entry : externalOps.values()) {
//            System.out.println(entry.studentID + " | Course: " + entry.courseID
//                    + " | Field: " + entry.column + " | Value: " + entry.newValue);
//        }
//        System.out.println();
//
//        System.out.println("Filtered Postgres Oplog Entries:");
//        for (OplogEntry entry : postgresOps.values()) {
//            System.out.println(entry.studentID + " | Course: " + entry.courseID
//                    + " | Field: " + entry.column + " | Value: " + entry.newValue);
//        }
//        System.out.println();

        // Merge logic
        for (String key : externalOps.keySet()) {
            OplogEntry externalEntry = externalOps.get(key);
            OplogEntry postgresEntry = postgresOps.get(key);

            boolean shouldUpdate = false;

            if (postgresEntry == null || externalEntry.timestamp.compareTo(postgresEntry.timestamp)>=0) {
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
                lastProcessedPostgresOpId+=1;

                System.out.println("Merged UPDATE to Postgres for: "
                        + externalEntry.studentID + " | Course: " + externalEntry.courseID
                        + " | Field: " + externalEntry.column + " | Value: " + externalEntry.newValue);
            }

        }

        // Update the OpID state file
        try (FileWriter writer = new FileWriter(opIdStateFile)) {
            writer.write(lastProcessedPostgresOpId + "," + lastProcessedExternalOpId);
        } catch (IOException e) {
            System.err.println("Error updating OpID state file: " + e.getMessage());
        }
    }
}