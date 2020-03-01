package com.cool.common.constant;

import org.jdom.Element;

public class SysConst {

	/** 应用根目录,系统启动后赋值，方可使用 */
	public static String WEBROOT_PATH = "WEBROOT_PATH";
	
	/** 数据库类型，用于定制不同的SQL语句 ,系统启动后赋值，方可使用*/
	public static String DATABASE_TYPE = "oracle"; 
	
	public static Element log4jElement = null;
	
	public static final String DOWNLOAD_DIR ="download";
	
	public static final String UPLOAD_DIR = "upload";
	
	public static final String FILE_UPLOAD_DIR = "fileUpload";
	
	public static final String OLAP_TEMPDATA_DIR = "olap/tempdata";
	
	
}
