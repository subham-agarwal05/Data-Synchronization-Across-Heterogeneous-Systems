package org.example;

import java.io.*;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import java.util.Map;

public class OpLog {

    private String OPLOG_FILE_PATH = "";
    //private static final String OPLOG_FILE_PATH = "/home/subham05/Desktop/NoSQL/NoSQLProject/src/data/mongo_oplog.csv";
    private int opIDCounter = 0;

    public OpLog(String OPLOG_FILE_PATH){
        // Constructor
        this.OPLOG_FILE_PATH = OPLOG_FILE_PATH;
        createOplogIfNotExists();
        //fetch last opID from the oplog file
        opIDCounter= getLastOpID();
    }

    public int getLastOpID() {
        try (BufferedReader br = new BufferedReader(new FileReader(this.OPLOG_FILE_PATH))) {
            String line;
            String lastLine = null;

            // Skip the header line
            br.readLine();

            // Read till the last line
            while ((line = br.readLine()) != null) {
                lastLine = line;
            }

            if (lastLine != null) {
                String[] parts = lastLine.split(",");
                return Integer.parseInt(parts[0]) + 1;
            }
        } catch (IOException | NumberFormatException e) {
            System.err.println("Error reading oplog file: " + e.getMessage());
        }

        return 1; // If the file only has a header or an error occurs
    }


    public void createOplogIfNotExists() {
        File file = new File(this.OPLOG_FILE_PATH);

        if (!file.exists()) {
            try {
                boolean created = file.createNewFile();
                if (created) {
                    try (FileWriter writer = new FileWriter(file)) {
                        // Write the header (adjust as needed)
                        writer.write("OpID,Timestamp,Table,Student ID, Course ID, Column,Operation,NewValue\n");
                        System.out.println("oplog file created: " + this.OPLOG_FILE_PATH);
                    }
                } else {
                    System.out.println("Failed to create oplog file.");
                }
            } catch (IOException e) {
                System.err.println("Error creating oplog file: " + e.getMessage());
            }
        } else {
            System.out.println("oplog file already exists at: " + this.OPLOG_FILE_PATH);
        }
    }

    public void writeToOplog(String table,String studentID, String courseID, String column, String operation, String newValue) {
        try (FileWriter writer = new FileWriter(this.OPLOG_FILE_PATH, true)) {
            // Write the log entry
            writer.write(opIDCounter + "," + ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "," + table + "," + studentID + "," + courseID + "," + column + "," + operation + "," + newValue + "\n");
            opIDCounter++;
        } catch (IOException e) {
            System.err.println("Error writing to oplog file: " + e.getMessage());
        }
    }

    public void readOplog(String path, Map<String, OplogEntry> map) {
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line = reader.readLine(); // Skip header
            while ((line = reader.readLine()) != null) {
                // Split the line into exactly 8 fields (to safely handle commas in newValue)
                String[] parts = line.split(",", 8);
                if (parts.length < 8) continue;

                int opID = Integer.parseInt(parts[0]);
                ZonedDateTime timestamp = ZonedDateTime.parse(parts[1]);
                String table = parts[2];
                String studentID = parts[3];
                String courseID = parts[4];
                String column = parts[5];
                String operation = parts[6];
                String newValue = parts[7];

                if (!"SET".equalsIgnoreCase(operation)) continue;

                // Composite key: table|studentID|courseID|column
                String key = table + "|" + studentID + "|" + courseID + "|" + column;
                OplogEntry entry = new OplogEntry(table, studentID, courseID, column, newValue, timestamp, operation);

                // Retain the latest entry based on timestamp
                if (!map.containsKey(key) || map.get(key).timestamp.isBefore(timestamp)) {
                    map.put(key, entry);
                }
            }
        } catch (IOException | NumberFormatException e) {
            System.err.println("Error reading oplog from " + path + ": " + e.getMessage());
        }
    }



}
