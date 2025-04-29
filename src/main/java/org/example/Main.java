package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.example.Hive.CsvToHiveImporter;
import org.example.Hive.HiveDAO;
import org.example.MongoDB.CsvToMongoImporter;
import org.example.Postgres_System.CsvToPostgresImporter;
import org.example.Postgres_System.PostgresDAO;
import org.example.MongoDB.MongoDAO;

public class Main {
    public static void main(String[] args) throws Exception {
        // Path to the testcase file (can be overridden via program arguments)
        String filePath = "src/data/testcase.in";

        // Instantiate DAOs
        DatabaseDAOInterface hiveDao = new HiveDAO();
        DatabaseDAOInterface sqlDao = new PostgresDAO();
        DatabaseDAOInterface mongoDao = new MongoDAO();

        //load csv into databases optionally
        System.out.println("Do you want to load csv into MongoDB? (y/n)");
        String loadCsv = System.console().readLine();
        if (loadCsv.equalsIgnoreCase("y")) {
            String csvFilePath = "src/data/student_course_grades.csv";
            CsvToMongoImporter.importCsv(csvFilePath);
        }
        System.out.println("Do you want to load csv into Hive? (y/n)");
        String loadHive = System.console().readLine();
        if (loadHive.equalsIgnoreCase("y")) {
            String csvFilePath = "src/data/student_course_grades.csv";
            CsvToHiveImporter.importCsv(csvFilePath);
        }
        System.out.println("Do you want to load csv into Postgres? (y/n)");
        String loadPostgres = System.console().readLine();
        if (loadPostgres.equalsIgnoreCase("y")) {
            String csvFilePath = "src/data/student_course_grades.csv";
            CsvToPostgresImporter.importCsv(csvFilePath);
        }


        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // Parse system name, operation, and argument string
                int dotIndex = line.indexOf('.');
                int parenIndex = line.indexOf('(', dotIndex);
                if (dotIndex < 0 || parenIndex < 0) {
                    System.err.println("Invalid command format: " + line);
                    continue;
                }

                String systemName = line.substring(0, dotIndex).trim();
                String operation = line.substring(dotIndex + 1, parenIndex).trim();
                String argsStr = line.substring(parenIndex + 1, line.lastIndexOf(')'));
                String[] parts = argsStr.split(",");

                DatabaseDAOInterface dao = getDao(systemName, hiveDao, sqlDao, mongoDao);

                switch (operation.toUpperCase()) {
                    case "GET":
                        if (parts.length >= 4) {
                            String tableName = parts[0].trim();
                            String column = parts[1].trim();
                            String studentId = parts[2].trim();
                            String courseId = parts[3].trim();
                            try {
                                String result = dao.getFieldValueByCompositeKey(studentId, courseId, column, tableName);
                                System.out.println(systemName + " GET => " + result + "\n");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            System.err.println("Invalid GET arguments: " + argsStr);
                        }
                        break;
                    case "SET":
                        if (parts.length >= 5) {
                            String tableName = parts[0].trim();
                            String column = parts[1].trim();
                            String studentId = parts[2].trim();
                            String courseId = parts[3].trim();
                            String newValue = parts[4].trim();
                            try {
                                dao.updateFieldByCompositeKey(studentId, courseId, column, newValue, tableName);
                                System.out.println(systemName + " SET => success \n");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            System.err.println("Invalid SET arguments: " + argsStr);
                        }
                        break;
                    case "MERGE":
                        if (parts.length >= 1) {
                            String sourceName = parts[0].trim();
                            try {
                                dao.Merge(sourceName);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            System.err.println("Invalid MERGE arguments: " + argsStr);
                        }
                        break;
                    default:
                        System.err.println("Unknown operation: " + operation);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static DatabaseDAOInterface getDao(
            String systemName,
            DatabaseDAOInterface hiveDao,
            DatabaseDAOInterface sqlDao,
            DatabaseDAOInterface mongoDao
    ) {
        if (systemName.equalsIgnoreCase("HIVE")) {
            return hiveDao;
        } else if (systemName.equalsIgnoreCase("SQL")) {
            return sqlDao;
        } else if (systemName.equalsIgnoreCase("MONGO")) {
            return mongoDao;
        } else {
            throw new IllegalArgumentException("Unknown system: " + systemName);
        }
    }
}