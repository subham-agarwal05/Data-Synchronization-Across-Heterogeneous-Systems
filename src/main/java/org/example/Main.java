//package org.example;
//
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoClients;
//import com.mongodb.client.MongoDatabase;
//import org.example.MongoDB.CsvToMongoImporter;
//import org.example.MongoDB.MongoDB;
//
//import java.util.Scanner;
//
//public class Main {
//    public static void main(String[] args) {
//        // Connection string (adjust if needed)
//        String uri = "mongodb://localhost:27017";
//        String csvPath= "src/data/student_course_grades.csv";
//        String oplogPath= "src/data/mongo_oplog.csv";
//
//        // Create a client
//        try (MongoClient mongoClient = MongoClients.create(uri)) {
//            // Connect to the database
//            MongoDatabase database = mongoClient.getDatabase("nosql");
//
//            System.out.println("Connected to database successfully");
//
//            //loading csv into the database
//            System.out.println("Do you want to load data into the database? (y/n)");
//            Scanner scanner = new Scanner(System.in);
//            String line = scanner.nextLine();
//            if (line.equalsIgnoreCase("y")) {
//                CsvToMongoImporter importer = new CsvToMongoImporter(mongoClient, "nosql", "student_course_grades");
//                importer.importCsv(csvPath);
//            }
//            MongoDB mongoDB = new MongoDB(mongoClient, "nosql");
////            mongoDB.updateDataByField("student_course_grades", "SID1310", "CSE020", "grade", "A");
//            mongoDB.mongoMerge("hive");
//
//
//
//
//        }
//    }
//}
