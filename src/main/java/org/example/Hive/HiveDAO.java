package org.example.Hive;

import java.sql.SQLException;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import org.example.DatabaseDAOInterface;
import org.example.OpLog;
import org.example.OplogEntry;

public class HiveDAO implements DatabaseDAOInterface {

    private static final String URL = "jdbc:hive2://localhost:10000/default";
    private static final String USER = "hive";
    private static final String PASSWORD = "";
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String[] externalOpLogPaths = {
            "src/data/mongo_oplog.csv",
            "src/data/postgres_oplog.csv"
    };

    private final BasicDataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private static final OpLog opLog = new OpLog("src/data/hive_oplog.csv");

    public HiveDAO() {
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(HIVE_DRIVER);
        dataSource.setUrl(URL);
        dataSource.setUsername(USER);
        dataSource.setPassword(PASSWORD);
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public String getFieldValueByCompositeKey(String studentID, String courseID, String fieldName, String tableName) throws SQLException {
        String sql = String.format(
                "SELECT %s FROM %s WHERE student_id = ? AND course_id = ?", fieldName, tableName
        );

        try {
            String value = jdbcTemplate.queryForObject(
                    sql,
                    new Object[]{studentID, courseID},
                    String.class
            );
            opLog.writeToOplog(tableName, studentID, courseID, fieldName, "GET", "NA");
            return value;
        } catch (Exception e) {
            throw new SQLException(String.format(
                    "No record found for Student ID '%s' and Course ID '%s'.", studentID, courseID
            ), e);
        }
    }

    @Override
    public void updateFieldByCompositeKey(String studentID, String courseID, String targetFieldName, String newValue, String tableName) throws SQLException {
        String sql = String.format(
                "INSERT OVERWRITE TABLE %s " +
                        "SELECT student_id, course_id, roll_no, email_id, " +
                        "CASE WHEN student_id = ? AND course_id = ? THEN ? ELSE %s END AS %s " +
                        "FROM %s",
                tableName, targetFieldName, targetFieldName, tableName
        );

        int rows = jdbcTemplate.update(
                sql,
                studentID,
                courseID,
                newValue
        );

        if (rows > 0) {
            opLog.writeToOplog(tableName, studentID, courseID, targetFieldName, "SET", newValue);
        } else {
            throw new SQLException(String.format(
                    "Update failed. No record found for Student ID '%s' and Course ID '%s'.", studentID, courseID
            ));
        }
    }

    @Override
    public void Merge(String source) throws Exception {
        String oplogPath;
        String target = "hive"; // Target database is now Hive

        if ("mongo".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[0];
        } else if ("sql".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[1];
        } else {
            System.out.println("Invalid source: " + source);
            return;
        }

        // File to store the last processed OpIDs for the specific source-target pair
        String opIdStateFile = "src/data/" + target.toLowerCase() + "_" + source.toLowerCase() + "_opid_state.txt";
        int lastProcessedHiveOpId = 0;
        int lastProcessedExternalOpId = 0;

        // Read the last processed OpIDs from the file
        File file = new File(opIdStateFile);
        if (file.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String[] opIds = reader.readLine().split(",");
                lastProcessedHiveOpId = Integer.parseInt(opIds[0]);
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

        DatabaseDAOInterface dao = new org.example.Hive.HiveDAO();
        Map<String, OplogEntry> externalOps = new HashMap<>();
        Map<String, OplogEntry> hiveOps = new HashMap<>();

        // Read oplogs and filter based on last processed OpIDs
        lastProcessedExternalOpId = opLog.readOplog(oplogPath, externalOps, lastProcessedExternalOpId);
        lastProcessedHiveOpId =opLog.readOplog("src/data/hive_oplog.csv", hiveOps, lastProcessedHiveOpId);

//        // Print filtered oplogs
//        System.out.println("Filtered External Oplog (" + source + ") Entries:");
//        for (OplogEntry entry : externalOps.values()) {
//            System.out.println(entry.studentID + " | Course: " + entry.courseID
//                    + " | Field: " + entry.column + " | Value: " + entry.newValue);
//        }
//        System.out.println();
//
//        System.out.println("Filtered Hive Oplog Entries:");
//        for (OplogEntry entry : hiveOps.values()) {
//            System.out.println(entry.studentID + " | Course: " + entry.courseID
//                    + " | Field: " + entry.column + " | Value: " + entry.newValue);
//        }
//        System.out.println();

        // Merge logic
        int maxExternalOpId = lastProcessedExternalOpId;
        int maxHiveOpId = lastProcessedHiveOpId;

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
                lastProcessedHiveOpId+=1;

                System.out.println("Merged UPDATE to Hive for: "
                        + externalEntry.studentID + " | Course: " + externalEntry.courseID
                        + " | Field: " + externalEntry.column + " | Value: " + externalEntry.newValue);
            }
        }

        // Update the OpID state file
        try (FileWriter writer = new FileWriter(opIdStateFile)) {
            writer.write(maxHiveOpId + "," + maxExternalOpId);
        } catch (IOException e) {
            System.err.println("Error updating OpID state file: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        try {
            Class.forName(HIVE_DRIVER);
            HiveDAO dao = new HiveDAO();
//            String value = dao.getFieldValueByCompositeKey("SID1310", "CSE020", "grade", "student_course_grades");
//            System.out.println(value);
//            dao.updateFieldByCompositeKey("SID1310", "CSE020", "grade", "D", "student_course_grades");
//            String value2 = dao.getFieldValueByCompositeKey("SID1310", "CSE020", "grade", "student_course_grades");
//            System.out.println(value2);
            dao.Merge("sql");
            dao.Merge("mongo");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
