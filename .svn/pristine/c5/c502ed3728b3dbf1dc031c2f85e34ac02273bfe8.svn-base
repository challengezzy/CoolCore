package com.cool.common.system;

import java.io.*;
import java.util.*;

import com.cool.common.vo.VectorMap;

/**
 * 服务端缓存
 * @author jerry
 * @date May 15, 2013
 */
public class CoolServerEnvironment implements Serializable {

    public static String[] APPMODULENAME;

    private static final long serialVersionUID = -3231827644600597388L;

    private static CoolServerEnvironment serverEnv = null; //

    private VectorMap map = null;

    private HashMap descmap = null;

    private String str_defaultdatasource = null;

    private String[] str_alldatasources = null;

    private CoolServerEnvironment() {
        map = new VectorMap();
        descmap = new HashMap();
    }

    public static CoolServerEnvironment getInstance() {
        if (serverEnv != null) {
            return serverEnv;
        }

        serverEnv = new CoolServerEnvironment(); ////.......
        return serverEnv;
    }

    public void put(Object _key, Object _value) {
        map.put(_key, _value);
    }

    public void put(Object _key, String _desc, Object _value) {
        map.put(_key, _value);
        descmap.put(_key, _desc);
    }

    public Object get(Object _key) {
        return map.get(_key); //
    }

    public String[] getKeys() {
        return map.getKeysAsString(); //
    }

    public String[] getRowValue(Object _key) {
        String[] str_return = new String[3];
        if (get(_key) == null) {
            return null;
        }

        str_return[0] = (String) _key;
        str_return[1] = descmap.get(_key) == null ? "" : descmap.get(_key).toString();
        str_return[2] = "" + get(_key);
        return str_return;
    }

    public String[][] getAllData() {
        String[] str_keys = getKeys();
        String[][] str_data = new String[str_keys.length][3];
        for (int i = 0; i < str_keys.length; i++) {
            str_data[i][0] = str_keys[i];
            String[] rowValue = getRowValue(str_keys[i]);
            if (rowValue != null) {
                str_data[i][1] = getRowValue(str_keys[i])[1];
                str_data[i][2] = getRowValue(str_keys[i])[2];
            } else { //
                str_data[i][1] = "";
                str_data[i][2] = "";
            } //
        }
        return str_data;
    }

    /**
     * 默认数据源名称
     * @return
     */
    public String getDefaultDataSourceName() {
        if (str_defaultdatasource != null) {
            return str_defaultdatasource;
        }

        str_defaultdatasource = (String) get("defaultdatasource"); //
        return str_defaultdatasource;
    }

    /**
     * 默认数据源名称
     * @return
     */
    public String[] getAllDataSourceNames() {
        if (str_alldatasources != null) {
            return str_alldatasources;
        }

        str_alldatasources = (String[]) get("ALLDATASOURCENAMES"); //
        return str_alldatasources;
    }

    public String[] getAppModuleName() {
        return APPMODULENAME;
    }
    
    public void setAppModuleName(String[] names) {
        APPMODULENAME=names;
    }

}
