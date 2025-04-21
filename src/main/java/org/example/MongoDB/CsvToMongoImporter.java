package org.example.MongoDB;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CsvToMongoImporter {
    private final MongoClient mongoClient;
    private final String databaseName;
    private final String collectionName;

    public CsvToMongoImporter(MongoClient mongoClient, String databaseName, String collectionName) {
        this.mongoClient = mongoClient;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    public void importCsv(String csvFilePath) {
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

}
