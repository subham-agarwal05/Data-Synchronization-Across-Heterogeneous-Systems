package org.example;

import java.sql.SQLException;

public interface DatabaseDAOInterface {
    String getFieldValueByCompositeKey(String studentID, String courseID, String fieldName, String tableName) throws Exception;

    void updateFieldByCompositeKey(String studentID, String courseID, String targetFieldName, String newValue, String tableName) throws Exception;
}