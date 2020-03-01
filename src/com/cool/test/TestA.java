package com.cool.test;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.httpclient.util.DateUtil;

public class TestA {
	
	public static void main(String[] args) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String lastRunTime = formatter.format(new Date());
		
		System.out.println(lastRunTime);
		System.out.println(DateUtil.formatDate(new Date(),"yyyy-MM-dd HH:mm:ss"));
	}

}
