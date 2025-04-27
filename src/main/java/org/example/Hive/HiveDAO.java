package org.example.Hive;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import org.example.DatabaseDAOInterface;
import org.example.OpLog;
import org.example.OplogEntry;

public class HiveDAO implements DatabaseDAOInterface{

    private static final String URL = "jdbc:hive2://localhost:10000/default";
    private static final String USER = "";
    private static final String PASSWORD = "";
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String[] externalOpLogPaths={
            "src/data/mongo_oplog.csv",
            "src/data/postgres_oplog.csv"
    };

    static OpLog opLog = new OpLog("src/data/hive_oplog.csv")  ;
    // 1. Get field value by composite key
    @Override
    public String getFieldValueByCompositeKey(String studentID, String courseID, String fieldName, String tableName) throws SQLException {
        // Query to fetch the field value using the composite key (student-ID, course-ID)
        String query = String.format("SELECT %s FROM %s WHERE student_id= ? AND course_id = ?", fieldName, tableName);
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, studentID);
            pstmt.setString(2, courseID);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
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

        String update = String.format("INSERT OVERWRITE TABLE %s " +
                                        "SELECT " +
                                        "student_id, course_id, roll_no, email_id, " +
                                        "CASE " +
                                            "WHEN student_id = ? " +
                                            "AND course_id  = ? THEN ? " +
                                            "ELSE %s " +
                                        "END AS %s " +
                                "FROM %s", tableName, targetFieldName, targetFieldName, tableName);
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(update)) {

            pstmt.setString(1, studentID);
            pstmt.setString(2, courseID);
            pstmt.setString(3, newValue);

            try {
                int rows = pstmt.executeUpdate();
                System.out.println("Updated successfully.");
                opLog.writeToOplog(tableName, studentID , courseID, targetFieldName, "SET", newValue);
            } catch (SQLException e) {
                throw new SQLException(String.format("Update failed. No record found for Student ID '%s' and Course ID '%s'.", studentID, courseID));
            }
        }
    }

    public static void hiveMerge(String source) throws Exception {
        String oplogPath;
        if ("mongo".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[0];
        } else if ("postgresql".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[1];
        } else {
            System.out.println("Invalid source: " + source);
            return;
        }

        DatabaseDAOInterface dao = new org.example.Hive.HiveDAO();
        // Map of latest ops from external and mongo oplogs
        Map<String, OplogEntry> externalOps = new HashMap<>();
        Map<String, OplogEntry> hiveOps = new HashMap<>();

        opLog.readOplog(oplogPath, externalOps);
        opLog.readOplog("src/data/hive_oplog.csv", hiveOps);

        //print both maps
        System.out.println("External Oplog Entries:");
        for (OplogEntry entry : externalOps.values()) {
            System.out.println(entry.studentID + " | Course: " + entry.courseID
                    + " | Field: " + entry.column + " | Value: " + entry.newValue);
        }

        System.out.println("Hive Oplog Entries:");
        for (OplogEntry entry : hiveOps.values()) {
            System.out.println(entry.studentID + " | Course: " + entry.courseID
                    + " | Field: " + entry.column + " | Value: " + entry.newValue);
        }

        for (String key : externalOps.keySet()) {
            OplogEntry externalEntry = externalOps.get(key);
            OplogEntry hiveEntry = hiveOps.get(key);

            boolean shouldUpdate = false;

            if (hiveEntry == null || externalEntry.timestamp.isAfter(hiveEntry.timestamp)) {
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

                System.out.println("Merged UPDATE to Hive for: "
                        + externalEntry.studentID + " | Course: " + externalEntry.courseID
                        + " | Field: " + externalEntry.column + " | Value: " + externalEntry.newValue);
            }
        }
        System.out.println("Merged Hive with " +source+  " successfully.");
    }

    public static void main(String[] args) {
        try {
            Class.forName(HIVE_DRIVER);
            DatabaseDAOInterface dao = new org.example.Hive.HiveDAO();
            String studentID = "SID1310";
            String courseID = "CSE020";
            String fieldName = "grade";
            String tableName = "student_course_grades";
//            String fieldValue = dao.getFieldValueByCompositeKey(studentID, courseID, fieldName, tableName);
//            System.out.println("Field Value: " + fieldValue);
//
//            String newValue = "A";
//            dao.updateFieldByCompositeKey(studentID, courseID, fieldName, newValue, tableName);
//            String updatedFieldValue = dao.getFieldValueByCompositeKey(studentID, courseID, fieldName, tableName);
//            System.out.println("Updated Field Value: " + updatedFieldValue);
            hiveMerge("postgresql");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}