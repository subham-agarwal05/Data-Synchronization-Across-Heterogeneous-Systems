package org.example.Postgres_System;

import org.example.OplogEntry;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.time.ZonedDateTime;
import java.util.ArrayList;

public class Oplog_PostgresDAO {

    private static final String URL = "jdbc:postgresql://localhost:5432/postgres_db";
    private static final String USER = "postgres";
    private static final String PASSWORD = "root";
    private static final String CSV_FILE_PATH = "src/data/postgres_oplog.csv";

    public static void createOplogTable() throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS postgres_oplog (" +
                "id SERIAL PRIMARY KEY, " +
                "timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                "table_name TEXT, " +
                "student_id TEXT, " +
                "course_id TEXT, " +
                "column_name TEXT, " +
                "new_value TEXT, " +
                "operation_type TEXT" +
                ")";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);
        }
    }

    public static void insertOplogEntry(OplogEntry entry) throws SQLException {
        createOplogTable();
        String insertSQL = "INSERT INTO postgres_oplog (table_name, student_id, course_id, column_name, new_value, timestamp, operation_type) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            pstmt.setString(1, entry.tableName);
            pstmt.setString(2, entry.studentID);
            pstmt.setString(3, entry.courseID);
            pstmt.setString(4, entry.column);
            pstmt.setString(5, entry.newValue);
            pstmt.setTimestamp(6, Timestamp.valueOf(entry.timestamp.toLocalDateTime()));
            pstmt.setString(7, entry.operationType);
            writeToCsv(entry); // Write to CSV
            pstmt.executeUpdate();
            System.out.println("Inserted oplog entry for student ID: " + entry.studentID +
                    ", course ID: " + entry.courseID + ", operation: " + entry.operationType);
        }
    }

    public static ArrayList<OplogEntry> readOplogEntries() throws SQLException {
        ArrayList<OplogEntry> entries = new ArrayList<>();
        String selectSQL = "SELECT * FROM postgres_oplog";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(selectSQL)) {
            while (rs.next()) {
                OplogEntry entry = new OplogEntry(
                        rs.getString("table_name"),
                        rs.getString("student_id"),
                        rs.getString("course_id"),
                        rs.getString("column_name"),
                        rs.getString("new_value"),
                        rs.getTimestamp("timestamp").toInstant().atZone(java.time.ZoneId.systemDefault()),
                        rs.getString("operation_type")
                );
                entries.add(entry);
            }
        }
        return entries;
    }

    public static void printOplogEntries() throws SQLException {
        ArrayList<OplogEntry> oplogEntries = readOplogEntries();
        for (OplogEntry entry : oplogEntries) {
            System.out.println("Table: " + entry.tableName + ", Student ID: " + entry.studentID +
                    ", Course ID: " + entry.courseID + ", Column: " + entry.column +
                    ", New Value: " + entry.newValue + ", Timestamp: " + entry.timestamp);
        }
    }

    public static void writeToCsv(OplogEntry entry) {
        File file = new File(CSV_FILE_PATH);
        boolean fileExists = file.exists();

        try (FileWriter writer = new FileWriter(file, true)) {
            // If the file doesn't exist, create it and write the header
            if (!fileExists) {
                file.getParentFile().mkdirs(); // Ensure the directory exists
                writer.write("OpID,Timestamp,Table,Student ID,Course ID,Column,Operation,NewValue\n");
            }

            // Get the next OpID from the database
            int nextOpId = getNextOpIdFromDatabase();

            // Write the log entry in the specified format
            writer.write(String.format("%d,%s,%s,%s,%s,%s,%s,%s\n",
                    nextOpId,
                    entry.timestamp.format(java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                    entry.tableName,
                    entry.studentID,
                    entry.courseID,
                    entry.column,
                    entry.operationType,
                    entry.newValue));
        } catch (IOException e) {
            System.err.println("Error writing to CSV file: " + e.getMessage());
        }
    }

    private static int getNextOpIdFromDatabase() {
        String query = "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM postgres_oplog";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getInt("next_id");
            }
        } catch (SQLException e) {
            System.err.println("Error retrieving next OpID from database: " + e.getMessage());
        }
        return 1; // Default to 1 if there's an error
    }

    public static void main(String[] args) {
        try {
            OplogEntry entry2 = new OplogEntry("student_course_grades", "SID1310", "CSE020", "grade", "A", ZonedDateTime.now(), "SET");
            insertOplogEntry(entry2);

            //OplogEntry entry2 = new OplogEntry("student_course_grades", "SID1310", "CSE102", "grade", "B", ZonedDateTime.now());
            //insertOplogEntry(entry2);

            printOplogEntries();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}