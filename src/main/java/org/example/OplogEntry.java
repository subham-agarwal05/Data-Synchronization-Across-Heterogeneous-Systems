package org.example;

import java.time.ZonedDateTime;

public class OplogEntry {
    public String tableName;
    public String studentID;
    public String courseID;
    public String column;
    public String newValue;
    public ZonedDateTime timestamp;
    public String operationType;

    public OplogEntry(String tableName,
                      String studentID,
                      String courseID,
                      String column,
                      String newValue,
                      ZonedDateTime timestamp,
                      String operationType) {
        this.tableName = tableName;
        this.studentID = studentID;
        this.courseID = courseID;
        this.column = column;
        this.newValue = newValue;
        this.timestamp = timestamp;
        this.operationType = operationType;
    }
}