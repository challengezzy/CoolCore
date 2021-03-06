package com.cool.dbaccess;


import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;
import org.jdom.Element;


import com.cool.common.logging.CoolLogger;
import com.cool.common.util.StringUtil;

/**
 * 数据源处理
 * @author James.W
 *
 */
public class DataSourceManager {
	private DataSourceManager(){
		
	}
	private static Logger logger=CoolLogger.getLogger(DataSourceManager.class);
	private static String default_ds=null;
	private static HashMap<String,DataSourceObject> _dsomap=new HashMap<String,DataSourceObject>();
	private static HashMap<String,HashMap<String, String>> _dssetup=new HashMap<String,HashMap<String, String>>();
	
	private static int DS_RCONN_TIMES=0;//修复中尝试次数
	private static int DS_RCONN_DELAY=0;//修复尝试间隔
	private static int DS_REPAIR_TIMES=0;//修复总尝试次数
	
	static{
		DS_RCONN_TIMES= 5;
		DS_RCONN_DELAY=6000;
		DS_REPAIR_TIMES=100;
	}
	
	/**
	 * 初始化数据源
	 * 替代NovaBootServlet内的对应功能
	 * @param es Nova2配置xml内的的数据源节点列表
	 */
	public static void initDS(List es){
		for (int i = 0; i < es.size(); i++) {
        	
            Element node =(Element)es.get(i); //
            String name = node.getAttributeValue("name").trim(); // 得到属性
            
            String type = node.getAttributeValue("type");
            String lookupName = node.getAttributeValue("lookupName");
            
            HashMap<String,String> dssetup=new HashMap<String,String>();
            dssetup.put("name", name);
            if(!StringUtil.isEmpty(type) && type.equalsIgnoreCase("JNDI")){
            	dssetup.put("type", "JNDI");
            	dssetup.put("lookupName", lookupName);
            	dssetup.put("url", lookupName);
            }else{
            	
                String driver = node.getChild("driver").getTextTrim();
                String url = node.getChild("url").getTextTrim();
                String initsize = node.getChild("initsize").getTextTrim();
                String poolsize = node.getChild("poolsize").getTextTrim();
                int li_initsize = Integer.parseInt(initsize); //
                int li_poolsize = Integer.parseInt(poolsize); //
                String validationQuery=(node.getChild("validationQuery")!=null)?node.getChild("validationQuery").getTextTrim():"SELECT COUNT(*) FROM DUAL";
            	
                //缓冲配置信息
            	dssetup.put("type", "DBCP");
                dssetup.put("driver", driver);
                dssetup.put("url", url);
                dssetup.put("initsize", initsize);
                dssetup.put("poolsize", poolsize);
                dssetup.put("validationQuery", validationQuery);
                
                //生产链接池控制对象
                DataSourceObject dso=new DataSourceObject(name,driver,url,li_initsize,li_poolsize,validationQuery,DS_RCONN_TIMES,DS_RCONN_DELAY,DS_REPAIR_TIMES);
                _dsomap.put(name, dso);
                
            }
            _dssetup.put(name, dssetup);
            
            if (i == 0) {
            	default_ds=name;
            }
            
        }
		
		
	}
	
	/**
	 * 初始化数据源
	 * 替代NovaBootServlet内的对应功能
	 * @param pros 配置列表
	 */
	public static void initDS(Properties[] pros){
		//TODO 
	}
	/**
	 * 初始化数据源
	 * 替代NovaBootServlet内的对应功能
	 * @param maps 配置列表
	 */
	public static void initDS(HashMap<String,String>[] maps){
		for(HashMap<String,String> map : maps){
			String name = (String) map.get("name");
			//change by caohenghui --start
			String type = (String) map.get("type");
			if(!StringUtil.isEmpty(type) && type.equalsIgnoreCase("JNDI")){
				if(_dssetup.containsKey(name)){
					_dssetup.remove(name);
				}
				String lookupName = map.get("lookupName");
            	map.put("url", lookupName);
            	_dssetup.put(name, map);
            //change by caohenghui --end
			}else{
				if(_dsomap.containsKey(name)){
					//数据源已存在，先销毁当前的
					DataSourceObject dso = (DataSourceObject) _dsomap.get(name);
					dso.destroy();
					_dsomap.remove(name);
				}
	            String driver = (String) map.get("driver");
	            String url = (String) map.get("url");
	            String initsize = (String) map.get("initsize");
	            String poolsize = (String) map.get("poolsize");
	            int li_initsize = Integer.parseInt(initsize); //
	            int li_poolsize = Integer.parseInt(poolsize); //
	            String validationQuery= (map.get("validationQuery")!=null)?((String)map.get("validationQuery")):"SELECT COUNT(*) FROM DUAL";
	            _dssetup.put(name, map);
	            //生产链接池控制对象
	            DataSourceObject dso=new DataSourceObject(name,driver,url,li_initsize,li_poolsize,validationQuery,DS_RCONN_TIMES,DS_RCONN_DELAY,DS_REPAIR_TIMES);
	            _dsomap.put(name, dso);
			}
		}
		
	}
	
	
	
	/**
	 * 从指定数据源获得数据库链接
	 * @param ds
	 * @return
	 * @throws Exception 
	 */
	public static Connection getConnection(String ds)throws SQLException{
		String _ds=(ds==null||ds.trim().equals(""))?default_ds:ds;
		HashMap<String,String> dssetup = _dssetup.get(_ds);
		if(dssetup == null){
			logger.error("获取指定的数据源名称配置信息为空，!datasourceName:"+ds+".");
			throw new SQLException("获取指定的数据源名称配置信息为空!");
		}
		
		
		String dsType = dssetup.get("type");
		
		if(!StringUtil.isEmpty(dsType)&&dsType.equalsIgnoreCase("JNDI")){
			String lookupName = dssetup.get("lookupName");
			try{
				Context initContext = new InitialContext();
//				Context envContext = (Context) initContext.lookup(contextName);
//				DataSource datasource = (DataSource) envContext.lookup(lookupName);
				DataSource datasource = (DataSource) initContext.lookup(lookupName);
				return datasource.getConnection();
			}catch(Exception e){
				logger.error("获取连接时产生错误!lookupName:"+lookupName+";"+e.getMessage());
				throw new SQLException("获取连接时产生错误;"+e.getMessage());
			}
		}else{
			DataSourceObject dso=(DataSourceObject)_dsomap.get(_ds);
			if(dso==null) throw new SQLException("没有建立合适的连接池！");
			
			if(!dso.isReady()){
				dso.doRepair();//尝试启动修复
				throw new SQLException("连接池当前不可用（初始化、修复中）！");
			}
			try{
				return dso.getConnection();			
			}catch(SQLException e){
				logger.error("获取连接池产生错误！"+e.getMessage());
				dso.doRepair();			
				throw e;
			}
		}
		
	}
	
	
	/**
	 * 获得所有的连接池的名称列表
	 * @return
	 */
	public static String[] getDataSources(){
		if(_dssetup.size()==0) return new String[]{};
		return (String[])_dssetup.keySet().toArray(new String[0]);		
	}
	
	/**
	 * 获得所有连接池的配置信息
	 * @return
	 */
	public static HashMap getDataSourcesSet(){
		return _dssetup;
	}
	
	/**
	 * 是否存在名为ds的数据源
	 * @param ds
	 * @return
	 */
	public static boolean hasDatasource(String ds){
		return _dsomap.containsKey(ds);
	}
	
	public static String getDataSourceUrl(String ds){
		if(_dssetup.size()==0) return null;
		String _ds=(ds==null||ds.trim().equals(""))?default_ds:ds;
		return ((HashMap)_dssetup.get(_ds)).get("url").toString();
	}
	
	/**
	 * 获得默认数据源
	 * @return 如果已经初始化过则返回正常数据源名称，否则直接返回null
	 */
	public static String getDefaultDS(){
		return default_ds;
	}
	
	
	
	
	/**
	 * 关闭连接管理器
	 */
	public static void destroy(){
		logger.info("终止数据库连接池管理器...");
		String[] keys=getDataSources();
		for(int i=0;i<keys.length;i++){
			logger.info("终止数据库连接池【"+keys[i]+"】...");
			DataSourceObject dso=(DataSourceObject)_dsomap.get(keys[i]);
			dso.destroy();
			_dsomap.remove(keys[i]);
			_dssetup.remove(keys[i]);
			logger.info("终止数据库连接池【"+keys[i]+"】结束。");
		}
		logger.info("终止数据库连接池管理器结束。");
	}
	
	public static void destroy(String dsName){
		try{
			
			DataSourceObject dso = (DataSourceObject) _dsomap.get(dsName);
			if(dso != null){
				dso.destroy();
				_dsomap.remove(dsName);
			}
			//add by caohenghui --start
			_dssetup.remove(dsName);
			//add by caohenghui --end
		}catch(Exception e){
			logger.info("",e);
		}

	}
	
}

/**
 * 池操作包装
 * @author James.W
 *
 */
class DataSourceObject{
	private static Logger logger=CoolLogger.getLogger(DataSourceObject.class);
	
	private String dsname=null; 
	private boolean ready=false;
	private boolean fixing=false;
	private boolean stop=false;
	private BasicDataSource ds=null;
	
	private DsRepair dsrepair=null;
	
	private int DS_RCONN_TIMES=0;//修复中尝试次数
	private int DS_RCONN_DELAY=0;//修复尝试间隔
	private int DS_REPAIR_TIMES=0;//修复总尝试次数
	
	public DataSourceObject(String dsname, String driver, String url, int initsize, int poolsize,
			String validationQuery,int DS_RCONN_TIMES,int DS_RCONN_DELAY,int DS_REPAIR_TIMES){
		this.DS_RCONN_TIMES=DS_RCONN_TIMES;
		this.DS_RCONN_DELAY=DS_RCONN_DELAY;
		this.DS_REPAIR_TIMES=DS_REPAIR_TIMES;
		
		this.dsname=dsname;
		
		this.ds=new BasicDataSource();
		this.ds.setDriverClassName(driver);
		this.ds.setUrl(url);
		this.ds.setMaxActive(poolsize);
		this.ds.setMinIdle(initsize);
		this.ds.setInitialSize(initsize);//add by xuzhilin 20120508
		this.ds.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		if(validationQuery != null){
			this.ds.setValidationQuery(validationQuery);
			this.ds.setMinEvictableIdleTimeMillis(1800000L);
			this.ds.setTimeBetweenEvictionRunsMillis(1800000L);
			this.ds.setNumTestsPerEvictionRun(3);
			this.ds.setTestOnBorrow(true);
			this.ds.setTestOnReturn(true);
			this.ds.setTestWhileIdle(true);
		}
		this.ds.setMaxWait(3000L);//最大等待时间
		
		this.ds.setDefaultAutoCommit(true);		
		try {
			// The BasicDataSource has lazy initialization
            // borrowing a connection will start the DataSource
            // and make sure it is configured correctly.
			Connection conn = this.ds.getConnection();
	        conn.close();
		} catch (Exception e) {
			logger.error("初始化数据库连接池[" + dsname + "][" + url + "]产生错误！",e);
		}
		
    	logger.info("成功初始化数据库连接池[" + dsname + "][" + url + "] 当前连接池的缓冲连接："+this.ds.getNumIdle());
    	this.ready=true;
    	this.fixing=false;
    	this.stop=false;
	}
	
	public int getDS_REPAIR_TIMES(){
		return this.DS_REPAIR_TIMES;
	}
	
	public void shutdown(){
		this.stop=true;
	}
	
	public boolean isReady(){
		return this.ready;
	}
	
	public boolean isFixing(){
		return this.fixing;
	}
	
	public void destroy(){
		try {
			this.destroyDS();
		} catch (SQLException e) {
			logger.error("销毁数据库连接池错误：[DS="+this.dsname+";ErrorCode="+e.getErrorCode()+"]"+e.getMessage());
		}finally{
			this.ds=null;
		}		
	}
	
	//销毁链接池管理对象，关闭线程。
	private void destroyDS()throws SQLException{
		if(this.dsrepair!=null){
			this.shutdown();
			int i=10;
			while(i>0){
				if(this.dsrepair.isAlive()){
					try{Thread.sleep(200);}catch(Exception e){}
				}
				i--;
			}
			this.dsrepair=null;//直接销毁了			
		}
		this.ds.close();
	}
	
	/**
	 * 获得数据库连接
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection()throws SQLException{
		try{
			Connection conn=this.ds.getConnection();	
			logger.debug("[调试信息（连接获取后）]当前调用连接池["+this.dsname+"]，当前池中共有连接["+(this.ds.getNumActive()+this.ds.getNumIdle())+"]个，空闲链接["+this.ds.getNumIdle()+"]个；");
			return conn;
		}catch(SQLException e){
			this.ready=false;
			logger.error("获取数据库链接错误：[DS="+this.dsname+";ErrorCode="+e.getErrorCode()+"]",e);
			throw e;			
		}
	}
	
	/**
	 * 线程处理
	 */
	public void doRepair(){
		if(!this.fixing){
			//当前没有执行修复线程，创建修复线程，并执行之。
			this.dsrepair=null;
			this.dsrepair=new DsRepair();
			this.dsrepair.start();
		}
	}
	
	
	class DsRepair extends Thread{
	
		/**
		 * 线程处理
		 */
		public void run(){
			if(!fixing){
				//当前没有执行修复线程
				doRepair();
			}
		}
		/**
		 * 修复方法
		 */
		private void doRepair(){
			fixing=true;
			Connection conn=null;
			for(int i=0;i<DS_RCONN_TIMES;i++){
				if(stop){
					fixing=false;
					return;
				}
				logger.info("尝试重新链接【延迟"+DS_RCONN_DELAY+"毫秒，进行"+i+"次尝试】...");    			
				try{
					if(DS_RCONN_DELAY>0l){
	    				Thread.sleep(DS_RCONN_DELAY);
	    			}
					conn=ds.getConnection(); 
					fixing=false;
					DS_REPAIR_TIMES=0;
					ready=true;    				
					return;    				
				}catch(SQLException ex){
					logger.error("获取数据库链接错误：[DS="+dsname+";ErrorCode="+ex.getErrorCode()+"]"+ex.getMessage());    				
				}catch(Exception err){
					logger.error("发生不可控制异常！",err);
					logger.error("发生不可控制异常！请查看日志。发生时间为【"+(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).format(new Date(System.currentTimeMillis()))+"】");
				}finally{
					try { conn.close(); } catch (Exception e) {	}
				}
			}
			CoolLogger.getLogger(this).error("经过尝试仍然不能连接到数据库！");
			fixing=false;//虽然没有修复成功也要恢复状态
			DS_REPAIR_TIMES++;//记录尝试修复的总次数
		}
	
	}
	
	public String getDsname() {
		return dsname;
	}
	
	
	
}
