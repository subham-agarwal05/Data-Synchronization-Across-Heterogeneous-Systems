package org.example.Postgres_System;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class CsvToPostgresImporter {

    // JDBC connection parameters
    private static final String URL = "jdbc:postgresql://localhost:5432/postgres_db";
    private static final String USER = "postgres";
    private static final String PASSWORD = "root";

    /**
     * Reads a CSV file, uses its filename as the target table name,
     * creates the table if it doesn't exist (with an ID column and other columns as TEXT),
     * and bulk-inserts its rows. Assumes the first row in the CSV
     * is the header and column names match exactly.
     *
     * @param csvFilePath path to the CSV file
     */
    public static void importCsv(String csvFilePath)
            throws SQLException, IOException, CsvValidationException {

        // Derive table name from file name (without extension)
        Path path = Paths.get(csvFilePath);
        String fileName = path.getFileName().toString();
        String tableName = fileName.substring(0, fileName.lastIndexOf('.')).toLowerCase();

        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            // Read header row to get column names
            String[] columns = reader.readNext();
            if (columns == null) {
                throw new IllegalArgumentException("CSV file is empty: " + csvFilePath);
            }

            // Build CREATE TABLE SQL with 'id' as primary key and other columns as TEXT
            StringBuilder createCols = new StringBuilder();
            createCols.append("id SERIAL PRIMARY KEY, ");
            for (int i = 0; i < columns.length; i++) {
                createCols.append("\"").append(columns[i]).append("\" TEXT");
                if (i < columns.length - 1) {
                    createCols.append(", ");
                }
            }
            String createTableSql = String.format(
                    "CREATE TABLE IF NOT EXISTS \"%s\" (%s)",
                    tableName, createCols);

            // Connect and ensure table exists
            try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
                 Statement stmt = conn.createStatement()) {
                stmt.execute(createTableSql);
                System.out.println("Table '" + tableName + "' is ready.");
            }

            // Build INSERT SQL (excluding ID column)
            StringBuilder colList = new StringBuilder();
            StringBuilder placeholders = new StringBuilder();
            for (int i = 0; i < columns.length; i++) {
                colList.append("\"").append(columns[i]).append("\"");
                placeholders.append("?");
                if (i < columns.length - 1) {
                    colList.append(", ");
                    placeholders.append(", ");
                }
            }
            String insertSql = String.format(
                    "INSERT INTO \"%s\" (%s) VALUES (%s)",
                    tableName, colList, placeholders);

            // Batch-insert data
            try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
                 PreparedStatement pstmt = conn.prepareStatement(insertSql)) {

                conn.setAutoCommit(false);
                String[] row;
                int count = 0;
                final int BATCH_SIZE = 1000;

                while ((row = reader.readNext()) != null) {
                    if (row.length != columns.length) {
                        throw new IllegalArgumentException(
                                "Row " + (count + 2) + " has " + row.length +
                                        " values; expected " + columns.length);
                    }
                    for (int i = 0; i < row.length; i++) {
                        pstmt.setString(i + 1, row[i]);
                    }
                    pstmt.addBatch();

                    if (++count % BATCH_SIZE == 0) {
                        pstmt.executeBatch();
                        conn.commit();
                    }
                }

                // Final batch commit
                pstmt.executeBatch();
                conn.commit();
                System.out.println("Imported " + count + " rows into table '" + tableName + "'.");
            }
        }
    }

    public static void main(String[] args) {
        // Directly set the CSV file path from the Dataset folder
        String csvFile = "src/data/student_course_grades.csv";

        try {
            Class.forName("org.postgresql.Driver");  // optional for JDBC4+
            importCsv(csvFile);
        } catch (ClassNotFoundException e) {
            System.err.println("PostgreSQL JDBC Driver not found.");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
