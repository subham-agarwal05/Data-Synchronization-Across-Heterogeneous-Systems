package org.example.Hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.nio.file.Paths;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CsvToHiveImporter {
    // Hive JDBC connection URL
    private static final String JDBC_URL = "jdbc:hive2://localhost:10000/default";
    private static final String USER = "";
    private static final String PASSWORD = "";
    // Hive JDBC driver class
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter the path to the CSV file to import:");
        String csvPath = sc.nextLine().trim();
        sc.close();

        // Derive table name from file name (without extension)
        String fileName = Paths.get(csvPath).getFileName().toString();
        String baseName = fileName.contains(".")
                ? fileName.substring(0, fileName.lastIndexOf('.'))
                : fileName;
        // Normalize table name: lowercase, replace invalid chars with underscore
        String tableName = baseName.toLowerCase().replaceAll("[^a-z0-9_]", "_");

        // Read header line to get column names
        String headerLine;
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            headerLine = br.readLine();
            if (headerLine == null) {
                throw new IllegalArgumentException("CSV file is empty");
            }
        }

        // Original header values
        String[] originalColumns = headerLine.split(",");

        // Sanitized column names
        String[] columnNames = Arrays.stream(originalColumns)
                .map(String::trim)
                .map(col -> col.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase())
                .toArray(String[]::new);

        // Build column definitions (all STRING)
        String columnDefs = IntStream.range(0, columnNames.length)
                .mapToObj(i -> columnNames[i] + " STRING")
                .collect(Collectors.joining(", "));

        Class.forName(HIVE_DRIVER);
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {

            // Create the table
            String createTableSQL = String.format(
                    "CREATE TABLE IF NOT EXISTS %s (%s) " +
                            "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
                            "STORED AS TEXTFILE",
                    tableName, columnDefs);
            System.out.println("Executing: " + createTableSQL);
            stmt.execute(createTableSQL);

            // Load data
            String loadDataSQL = String.format(
                    "LOAD DATA LOCAL INPATH '%s' INTO TABLE %s",
                    "/data/" + fileName, tableName);
            System.out.println("Executing: " + loadDataSQL);
            stmt.execute(loadDataSQL);

            // Build and execute a generic header-removal overwrite
            String selectColumns = String.join(", ", columnNames);
            String headerFilter = IntStream.range(0, columnNames.length)
                    .mapToObj(i -> String.format("%s <> '%s'", columnNames[i], originalColumns[i].trim()))
                    .collect(Collectors.joining(" AND "));
            String removeHeaderSQL = String.format(
                    "INSERT OVERWRITE TABLE %s SELECT %s FROM %s WHERE %s",
                    tableName, selectColumns, tableName, headerFilter);
            System.out.println("Executing: " + removeHeaderSQL);
            stmt.execute(removeHeaderSQL);

            System.out.println("Table '" + tableName + "' created, data loaded, header removed.");
        }
    }
}
