package com.example.demo.bean;

import java.util.ArrayList;

public class JsonBean {

    private String dbName;

    private ArrayList<ColumnValue> columnValueList;

    private String tableName;

    public String getDbName() {
        return dbName;
    }

    public ArrayList<ColumnValue> getColumnValueList() {
        return columnValueList;
    }

    public String getTableName() {
        return tableName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setColumnValueList(ArrayList<ColumnValue> columnValueList) {
        this.columnValueList = columnValueList;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
