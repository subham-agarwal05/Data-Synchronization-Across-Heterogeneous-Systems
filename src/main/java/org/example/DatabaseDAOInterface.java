package org.example;

public interface DatabaseDAOInterface {
    String getFieldValueByCompositeKey(String studentID, String courseID, String fieldName, String tableName) throws Exception;

    void updateFieldByCompositeKey(String studentID, String courseID, String targetFieldName, String newValue, String tableName) throws Exception;

    void Merge(String source) throws Exception;
}