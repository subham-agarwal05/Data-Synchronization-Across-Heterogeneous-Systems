package org.example.Hive;

import java.sql.SQLException;
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
        if ("mongo".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[0];
        } else if ("postgresql".equalsIgnoreCase(source)) {
            oplogPath = externalOpLogPaths[1];
        } else {
            System.out.println("Invalid source: " + source);
            return;
        }

        Map<String, OplogEntry> externalOps = new HashMap<>();
        Map<String, OplogEntry> hiveOps = new HashMap<>();

        opLog.readOplog(oplogPath, externalOps);
        opLog.readOplog("src/data/hive_oplog.csv", hiveOps);

        System.out.println("External Oplog Entries:");
        externalOps.values().forEach(entry -> System.out.println(
                entry.studentID + " | Course: " + entry.courseID
                        + " | Field: " + entry.column + " | Value: " + entry.newValue
        ));

        System.out.println("Hive Oplog Entries:");
        hiveOps.values().forEach(entry -> System.out.println(
                entry.studentID + " | Course: " + entry.courseID
                        + " | Field: " + entry.column + " | Value: " + entry.newValue
        ));

        for (Map.Entry<String, OplogEntry> extEntry : externalOps.entrySet()) {
            OplogEntry externalEntry = extEntry.getValue();
            OplogEntry hiveEntry = hiveOps.get(extEntry.getKey());

            if (hiveEntry == null || externalEntry.timestamp.isAfter(hiveEntry.timestamp)) {
                updateFieldByCompositeKey(
                        externalEntry.studentID,
                        externalEntry.courseID,
                        externalEntry.column,
                        externalEntry.newValue,
                        externalEntry.tableName
                );
                System.out.println("Merged UPDATE to Hive for: "
                        + externalEntry.studentID + " | Course: " + externalEntry.courseID
                        + " | Field: " + externalEntry.column + " | Value: " + externalEntry.newValue
                );
            }
        }

        System.out.println("Merged Hive with " + source + " successfully.");
    }

    public static void main(String[] args) {
        try {
            Class.forName(HIVE_DRIVER);
            HiveDAO dao = new HiveDAO();
            String value = dao.getFieldValueByCompositeKey("SID1310", "CSE020", "grade", "student_course_grades");
            System.out.println(value);
            dao.updateFieldByCompositeKey("SID1310", "CSE020", "grade", "D", "student_course_grades");
            String value2 = dao.getFieldValueByCompositeKey("SID1310", "CSE020", "grade", "student_course_grades");
            System.out.println(value2);
            //dao.Merge("postgresql");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
