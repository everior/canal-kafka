package com.example.demo.bean;

public class ColumnValue {

    private String columnName;

    private String columnValue;

    private Boolean isValid;

    public String getColumnName() {
        return columnName;
    }

    public String getColumnValue() {
        return columnValue;
    }

    public Boolean getValid() {
        return isValid;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setColumnValue(String columnValue) {
        this.columnValue = columnValue;
    }

    public void setValid(Boolean valid) {
        isValid = valid;
    }
}
