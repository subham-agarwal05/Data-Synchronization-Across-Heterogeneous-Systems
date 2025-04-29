package org.example.Hive;

import java.nio.file.Paths;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

public class CsvToHiveImporter {
    // Hive JDBC connection URL
    private static final String JDBC_URL = "jdbc:hive2://localhost:10000/default";
    private static final String USER = "hive";
    private static final String PASSWORD = "";
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    private static final BasicDataSource dataSource;
    private static final JdbcTemplate jdbcTemplate;

    static {
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(HIVE_DRIVER);
        dataSource.setUrl(JDBC_URL);
        dataSource.setUsername(USER);
        dataSource.setPassword(PASSWORD);
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public static void importCsv(String csvPath) throws Exception {

        // Derive table name from file name (without extension)
        String fileName = Paths.get(csvPath).getFileName().toString();
        String baseName = fileName.contains(".")
                ? fileName.substring(0, fileName.lastIndexOf('.'))
                : fileName;
        String tableName = baseName.toLowerCase().replaceAll("[^a-z0-9_]", "_");

        // Read header line to get column names
        String headerLine;
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            headerLine = br.readLine();
            if (headerLine == null) {
                throw new IllegalArgumentException("CSV file is empty");
            }
        }

        String[] originalColumns = headerLine.split(",");
        String[] columnNames = Arrays.stream(originalColumns)
                .map(String::trim)
                .map(col -> col.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase())
                .toArray(String[]::new);

        // Build SQL statements
        String columnDefs = IntStream.range(0, columnNames.length)
                .mapToObj(i -> columnNames[i] + " STRING")
                .collect(Collectors.joining(", "));

        String createTableSQL = String.format(
                "CREATE TABLE IF NOT EXISTS %s (%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE",
                tableName, columnDefs);
        System.out.println("Executing: " + createTableSQL);
        jdbcTemplate.execute(createTableSQL);

        String loadDataSQL = String.format(
                "LOAD DATA LOCAL INPATH '%s' INTO TABLE %s",
                csvPath.replace("\\", "/"), tableName);
        System.out.println("Executing: " + loadDataSQL);
        jdbcTemplate.execute(loadDataSQL);

        String selectColumns = String.join(", ", columnNames);
        String headerFilter = IntStream.range(0, columnNames.length)
                .mapToObj(i -> String.format("%s <> '%s'", columnNames[i], originalColumns[i].trim()))
                .collect(Collectors.joining(" AND "));
        String removeHeaderSQL = String.format(
                "INSERT OVERWRITE TABLE %s SELECT %s FROM %s WHERE %s",
                tableName, selectColumns, tableName, headerFilter);
        System.out.println("Executing: " + removeHeaderSQL);
        jdbcTemplate.execute(removeHeaderSQL);

        System.out.println("Table '" + tableName + "' created, data loaded, header removed.");
    }
}