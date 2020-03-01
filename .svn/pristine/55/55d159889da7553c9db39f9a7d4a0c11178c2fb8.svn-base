package com.cool.common.vo;

import java.io.*;


/**
 * 数据集的结构（字段列表，二维数据列表）
 *
 */
public class TableDataStruct implements Serializable {

    private static final long serialVersionUID = 7758843430724165925L;

    private String fromtablename = null; //从哪一个取的数,只取第一列的,不支持从多个表取数,除非写视图!!

    private String[] table_body_type = null;//字段类型

    private String[] table_header = null;//表头结构

    private String[][] table_body = null; //表体结构
    
    private int[] table_body_type_precision = null;//列精度
    
    private int[] table_body_type_scale = null;//列宽度,指定列的小数点右边的位数
    
    public int[] getTable_body_type_precision() {
		return table_body_type_precision;
	}

	public void setTable_body_type_precision(int[] table_body_type_precision) {
		this.table_body_type_precision = table_body_type_precision;
	}

	public int[] getTable_body_type_scale() {
		return table_body_type_scale;
	}

	public void setTable_body_type_scale(int[] table_body_type_scale) {
		this.table_body_type_scale = table_body_type_scale;
	}
	//add by zhangzz 2011-12-26 end

    public String[][] getTable_body() {
        return table_body;
    }

    public void setTable_body(String[][] table_body) {
        this.table_body = table_body;
    }

    public String[] getTable_header() {
        return table_header;
    }

    public void setTable_header(String[] table_header) {
        this.table_header = table_header;
    }

    public String[] getTable_body_type() {
        return table_body_type;
    }

    public void setTable_body_type(String[] table_body_type) {
        this.table_body_type = table_body_type;
    }

    public String getFromtablename() {
        return fromtablename;
    }

    public void setFromtablename(String fromtablename) {
        this.fromtablename = fromtablename;
    }

}