package com.cool.dbaccess.adapter;

import java.util.*;

public class DBExplainerCache  {

    private static DBExplainerCache dbExplainerCache = null;

    private HashMap tableMap = null;

    private HashMap columnMap = null;

    private DBExplainerCache() {
        tableMap = new HashMap();
        columnMap = new HashMap();
    }

    public static DBExplainerCache getInstance() {
        if (dbExplainerCache != null) {
            return dbExplainerCache;
        }
        dbExplainerCache = new DBExplainerCache();
        return dbExplainerCache;
    }

    public void putTableName(String _tableName, String _tableLocalName) {
        tableMap.put(_tableName.toUpperCase(), _tableLocalName);
    }

    public void putColumnName(String _tableName, String _columnName, String _columnLocalName) {
        columnMap.put(_columnName.toUpperCase() + "@" + _tableName.toUpperCase(), _columnLocalName);
    }

    public String getTableLocalName(String _tableName) {
        return (String) tableMap.get(_tableName.toUpperCase());
    }

    public String getColumnLocalName(String _tableName, String _columnName) {
        return (String) columnMap.get(_columnName.toUpperCase() + "@" + _tableName.toUpperCase());
    }

}
