package com.cool.common.logging;


import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.jdom.Element;

import com.cool.common.constant.SysConst;


/**
 * 日志文件.所有的Logger为coollogger的子Logger
 *
 */
public class CoolLogger {
	//是否初始化
	private static boolean isInit=false;
	
	/**
	 * 获得指定的类的日志
	 * 由本方法可以保证，所有的日志都是统一的采用本类设置的日志格式。
	 * @return
	 */
	public static Logger getLogger() {		
		return getLogger("Unknown Class");
	}

	/**
	 * 获得指定的类的日志
	 * 由本方法可以保证，所有的日志都是统一的采用本类设置的日志格式。
	 * @param obj
	 * @return
	 */
	public static Logger getLogger(Object obj) {
		return getLogger(obj.getClass());
	}

	/**
	 * 获得指定的类的日志
	 * 由本方法可以保证，所有的日志都是统一的采用本类设置的日志格式。
	 * @param cls
	 * @return
	 */
	public static Logger getLogger(Class<?> cls) {
		return getLogger(cls.getName());
	}
	
	/**
	 * 获得指定的类的日志
	 * 由本方法可以保证，所有的日志都是统一的采用本类设置的日志格式。
	 * @param cls
	 * @return
	 */
	public static Logger getLogger(String cls) {
		if(!isInit) {
			initLogger();
		}
		return Logger.getLogger(cls);
	}

	

	//禁止实例化
	private CoolLogger() {
	}
	
	public static boolean isInit(){
		return isInit;
	}
	
	public static void initLogger(){
		
		//默认配置
		String logpath = SysConst.WEBROOT_PATH+"/log";
		String level = "debug";
		String outputtype = "3";
    	
		
    	if(SysConst.log4jElement == null){
    		System.err.println("***************配置文件中未设置日志配置***************");
    		System.err.print("配置示例：\n<log4j>\n"+"\t<level>debug</level>\n\t<outputtype>3</outputtype>\n\t<clspath>com</clspath>\n</log4j>\n");
    	}else{
    		
    		Element logElement = SysConst.log4jElement;
    		logpath = logElement.getChildText("logpath");
    		//日志文件路径
    		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    		String lastStartTime = sdf.format(new Date());
    		logpath= (logpath + "/"+ lastStartTime).replaceAll("//", "/");
    		
    		level = logElement.getChildText("rootlevel");
    		outputtype = logElement.getChildText("outputtype");
    		
    		//配置rootlogger信息
    		initLogger(level,outputtype,logpath,"");
    		
    		Element clsLevelE = logElement.getChild("classlevels");
    		
    		String clspath = "";
    		@SuppressWarnings("unchecked")
			List<Element> logLevels = clsLevelE.getChildren("loglevel");
    		for (int i = 0; i < logLevels.size(); i++) {
				if (logLevels.get(i) instanceof Element) {
					Element levset = (Element) logLevels.get(i);
					
					clspath = levset.getAttributeValue("clspath");
					level = levset.getAttributeValue("level");
					
					initLogger(level,outputtype,logpath,clspath);
				}
    		}
    	}
    	
	    
		
		isInit=true;
    }
    
	//真正的初始化
	private static void initLogger(String level,String type,String path,String cls) {
		//日志目录初始化
  		File log_dir_file = new File(path);

  		if (!log_dir_file.exists()) {
			log_dir_file.mkdir();
		}
		
		//初始化日志
		Logger log=null;
		//判断是否客户端
		
		if( cls==null || cls.trim().equals("") || cls.trim().equals("N/A") ){
			log = Logger.getRootLogger();
		}else{
			log = Logger.getLogger(cls);
		}
		   
		// outputtype:
		// 1:只输出控制台不输出文件
		// 2:只输出文件,不输出控件台
		// 3:同时输出控制台与文件
		log.setAdditivity(false);
		Appender appender = null;
		
		log.removeAllAppenders();
		
		PatternLayout patten = new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %p %m <%C:%L> %n");
		if (type == null || type.equalsIgnoreCase("1")) {
			try{
				appender = new ConsoleAppender(patten, ConsoleAppender.SYSTEM_OUT);
				log.addAppender(appender);
				log.info("包路径=["+cls+"]设置日志控制台输出。");
			}catch(Throwable e){
				e.printStackTrace();
			}
		}else if (type.equalsIgnoreCase("2")) {
			try {
				String fpath=(path + "/" + "CoolLog.log").replaceAll("//", "/");
				appender = new DailyRollingFileAppender(patten, fpath, "'.'yyyy-MM-dd");
				((DailyRollingFileAppender) appender).setAppend(true); //
				log.addAppender(appender);
				log.info("包路径=["+cls+"]设置日志文件输出，位置【"+fpath+"】。");
			} catch (IOException e) {
				System.out.println("初始化log4j时,生成日志文件错误.");
				e.printStackTrace();
			}
		} else {
			try {
				appender = new ConsoleAppender(patten, ConsoleAppender.SYSTEM_OUT);
				log.addAppender(appender);
				log.info("包路径=["+cls+"]设置日志控制台输出。");
			} catch (Throwable e) {
				e.printStackTrace();
			}
			try {
				String fpath=(path + "/" + "CoolLog.log").replaceAll("//", "/");
				appender = new DailyRollingFileAppender(patten, fpath, "'.'yyyy-MM-dd");
				((DailyRollingFileAppender) appender).setAppend(false); 
				log.addAppender(appender);
				log.info("包路径=["+cls+"]设置日志文件输出，位置【"+ fpath +"】。");
			} catch (Throwable e) {
				System.out.println("初始化log4j时,生成日志文件错误.");
				e.printStackTrace();
			}			
		}
		
		//初始化日志级别
		initLogLevel(log,level);		
		
	}
	
	private static void initLogLevel(Logger log,String loglevel) {
		//DEBUG，INFO，WARN，ERROR，FATAL
		//System.out.println("输入的："+loglevel);
		Level level = Level.INFO;
		if (loglevel == null)
			level = Level.INFO;
		else if (loglevel.equalsIgnoreCase("DEBUG"))
			level = Level.DEBUG;
		else if (loglevel.equalsIgnoreCase("INFO"))
			level = Level.INFO;
		else if (loglevel.equalsIgnoreCase("ERROR"))
			level = Level.ERROR;
		else if (loglevel.equalsIgnoreCase("FATAL"))
			level = Level.FATAL;
		else if (loglevel.equalsIgnoreCase("WARN"))
			level = Level.WARN;
		else
			level = Level.INFO;
		
		log.setLevel(level);
	}

	
}
