package com.cool.common.util;

import com.cool.common.constant.DMOConst;
import com.cool.common.constant.SysConst;


public class DBUtil {
	
	public static String getInsertId(){
		String str = "";
		if(SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_ORACLE))
		{
			str = "ID,";
		}
		else if(SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_SQLSERVER)||SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_DB2))
		{
			str = "";
		}
		
		return str;
	}
	
	/**
	 * 根据SEQ名称获取下一个值
	 * @param seqName
	 * @return
	 */
	public static String getSeqValue(String seqName){
		String str = "";
		if(SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_ORACLE))
		{
			str = seqName +".NEXTVAL,";
		}
		else if(SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_SQLSERVER)||SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_DB2))
		{
			str = "";
		}
		return str;
	}
	
	/**
	 * 根据表名获取下一个值 
	 * @param tableName
	 * @return
	 */
	public static String getTableSeq(String tableName){
		String str = "";
		if(SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_ORACLE))
		{
			str = "S_" + tableName +".NEXTVAL,";
		}
		else if(SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_SQLSERVER)||SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_DB2))
		{
			str = "";
		}
		return str;
	}
	
	public static String getPkName(String pkName){
		String str = "";
		if(SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_ORACLE))
		{
			str = pkName+",";
		}
		else if(SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_SQLSERVER)||SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_DB2))
		{
			//oracle 中的主键要求必须是自动增长的
			str = "";
		}
		return str;
	}
	
	/**
	 * 为了满足各种不同的需要，因此这里只返回字符串，不带“,".例如应用在update或者delete语句中
	 * @return
	 */
	public static String getSysDate(){
		String str = "";
		if(SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_ORACLE))
		{
			str = "SYSDATE";
		}
		else if(SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_SQLSERVER)||SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_DB2))
		{
			str = "GETDATE()";
		}
		return str;
	}
	
	/** 时间字符串格式必须为 YYYY-MM-dd hh24:mi:ss */
	public static String getDateValue(String strDate){
		String str = "";
		if(SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_ORACLE))
		{
			str = "to_date(" + strDate + ",'YYYY-MM-dd hh24:mi:ss')";
		}
		else if(SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_SQLSERVER)||SysConst.DATABASE_TYPE.equals(DMOConst.DATABASE_TYPE_DB2))
		{
			str = "GETDATE()";
		}
		return str;
	}
	

}
