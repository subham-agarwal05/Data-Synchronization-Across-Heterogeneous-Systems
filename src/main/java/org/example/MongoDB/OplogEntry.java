package org.example.MongoDB;

import java.time.ZonedDateTime;

public class OplogEntry {
    String tableName;
    String studentID;
    String courseID;
    String column;
    String newValue;
    ZonedDateTime timestamp;

    public OplogEntry(String tableName,
                      String studentID,
                      String courseID,
                      String column,
                      String newValue,
                      ZonedDateTime timestamp) {
        this.tableName = tableName;
        this.studentID = studentID;
        this.courseID = courseID;
        this.column = column;
        this.newValue = newValue;
        this.timestamp = timestamp;
    }
}