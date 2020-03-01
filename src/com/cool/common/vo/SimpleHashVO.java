package com.cool.common.vo;

import java.util.HashMap;
import java.util.Map;

/**
 * 简单HashVo对象,一般用于在Client展示
 * @author jerry
 * @date Sep 18, 2013
 */
public class SimpleHashVO
{
	private Map<Object,Object> tableNameMap = new HashMap<Object, Object>();
	private Map<Object,Object> dataMap = new HashMap<Object, Object>();
	
	public SimpleHashVO(HashVO hashVO){
		if(hashVO == null)
			throw new IllegalArgumentException("hashVO不能为null");
		
		String[] keys = hashVO.getKeys();
		for(int i=0;i<keys.length;i++){
			String key = keys[i];
			tableNameMap.put(key, hashVO.getTableName(key));
			dataMap.put(key, hashVO.getObjectValue(key));
		}
	}
	
	
	public Map<Object,Object>  getTableNameMap()
	{
	
		return tableNameMap;
	}


	public void setTableNameMap(Map<Object,Object>  tableNameMap)
	{
	
		this.tableNameMap = tableNameMap;
	}


	public Map<Object,Object>  getDataMap()
	{
	
		return dataMap;
	}
	public void setDataMap(Map<Object,Object>  dataMap)
	{
	
		this.dataMap = dataMap;
	}
	
}