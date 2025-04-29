package org.example.MongoDB;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoClients;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CsvToMongoImporter {
    private static final String uri = "mongodb://localhost:27017";
    private static final MongoClient mongoClient = MongoClients.create(uri);
    private static final String databaseName = "nosql";
    private static final String collectionName = "student_course_grades";

    public static void importCsv(String csvFilePath) {
        // Get the database and collection
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            String[] headers = null;

            while ((line = br.readLine()) != null) {
                // Trim and skip blank lines
                if (line.trim().isEmpty()) {
                    continue;
                }

                String[] values = line.split(",");

                if (headers == null) {
                    headers = values;
                } else {
                    if (values.length < headers.length) {
                        System.out.println("Skipping line with missing columns: " + line);
                        continue;
                    }

                    Document doc = new Document();
                    for (int i = 0; i < headers.length; i++) {
                        doc.append(headers[i].trim(), values[i].trim());
                    }
                    collection.insertOne(doc);
                }
            }

            System.out.println("CSV data inserted into MongoDB.");
        } catch (IOException e) {
            System.err.println("Error reading CSV: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        // Path to the CSV file
        String csvFilePath = "src/data/student_course_grades.csv";

        try {
            importCsv(csvFilePath);

            System.out.println("CSV import completed successfully.");
        } catch (Exception e) {
            System.err.println("An error occurred during the import process: " + e.getMessage());
            e.printStackTrace();
        }
    }
}