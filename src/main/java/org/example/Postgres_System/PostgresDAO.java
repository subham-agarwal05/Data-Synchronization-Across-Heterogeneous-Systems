package org.example.Postgres_System;

import java.sql.*;
import java.time.ZonedDateTime;

import org.example.MongoDB.OplogEntry;
public class PostgresDAO {

    private static final String URL = "jdbc:postgresql://localhost:5432/postgres_db";
    private static final String USER = "postgres";
    private static final String PASSWORD = "root";

    // 1. Get field value by composite key
    public static String getFieldValueByCompositeKey(String studentID, String courseID, String fieldName, String tableName) throws SQLException {
        // Query to fetch the field value using the composite key (student-ID, course-ID)
        String query = String.format("SELECT \"%s\" FROM %s WHERE \"student-ID\" = ? AND \"course-id\" = ?", fieldName, tableName);
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, studentID);
            pstmt.setString(2, courseID);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    // Log the operation to the oplog
                    OplogEntry entry = new OplogEntry(tableName, studentID, courseID, fieldName, rs.getString(fieldName), ZonedDateTime.now(), "GET");
                    Oplog_PostgresDAO.insertOplogEntry(entry);
                    return rs.getString(fieldName);
                } else {
                    throw new SQLException(String.format("No record found for Student ID '%s' and Course ID '%s'.", studentID, courseID));
                }
            }
        }
    }


    // 2. Update field value by composite key
    public static void updateFieldByCompositeKey(String studentID, String courseID, String targetFieldName, String newValue, String tableName) throws SQLException {
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
                OplogEntry entry = new OplogEntry(tableName, studentID, courseID, targetFieldName, newValue, ZonedDateTime.now(), "UPDATE");
                Oplog_PostgresDAO.insertOplogEntry(entry);
                System.out.println("Updated successfully.");
            }
        }
    }

    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");

            String studentID = "SID1033";
            String courseID = "CSE016";
            String fieldName = "grade";
            String tableName = "student_course_grades";
            String fieldValue = getFieldValueByCompositeKey(studentID, courseID, fieldName, tableName);
            System.out.println("Field Value: " + fieldValue);

            String newValue = "B+";
            updateFieldByCompositeKey(studentID, courseID, fieldName, newValue, tableName);
            String updatedFieldValue = getFieldValueByCompositeKey(studentID, courseID, fieldName, tableName);
            System.out.println("Updated Field Value: " + updatedFieldValue);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}