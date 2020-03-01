package com.cool.dbaccess;

import java.sql.*;
import java.util.*;


/**
 * 会话工厂,非常重要,是一个单例模式,在这里存储了各个线程的数据库连接与客户端变量的信息!!
 * @author user
 *
 */
public class DBSessionFactory {

    private static DBSessionFactory factory = new DBSessionFactory();

    private static HashMap dbConnectionMap = new HashMap(); //指定数据源存储对象

    private static HashMap clientEnvMap = new HashMap(); //存储客户端环境变量的HashMap

    private DBSessionFactory() {
    }

    public static DBSessionFactory getInstance() {
        return factory;
    }

    /**
     * 取得指定数据源，并参与同一事务
     * @param thread
     * @param _dsname
     * @return
     * @throws SQLException
     */
    public synchronized DBConnection getConnection(Thread _thread, String _dsname) throws SQLException {
        Object obj = dbConnectionMap.get(_thread); //指定数据源存储对象!!
        if (obj != null) { //如果找到了该线程的对象!!
            HashMap map = (HashMap) obj;
            Object objConn = map.get(_dsname);
            if (objConn != null) {
                return (DBConnection) objConn; //
            } else {
                DBConnection conn = new DBConnection(_dsname); //创建指定数据源
                map.put(_dsname, conn); //
                return conn; //
            }
        } else {
            HashMap map = new HashMap();
            DBConnection conn = new DBConnection(_dsname); //创建指定数据源
            map.put(_dsname, conn); //
            dbConnectionMap.put(_thread, map); //
            return conn; //
        }
    }

    /**
     * @param _thread
     * @param _dsName
     * @return
     */
    protected synchronized boolean isGetConnection(Thread _thread) {
        return dbConnectionMap.containsKey(_thread); //
    }

    protected synchronized DBConnection[] GetAllConnections(Thread _thread) {
        Object obj = dbConnectionMap.get(_thread); //指定数据源存储对象!!
        if (obj == null) {
            return null;
        } else {
            Vector vector = new Vector();
            HashMap map = (HashMap) obj; //
            return (DBConnection[]) (new ArrayList(map.values()).toArray(new DBConnection[0])); //
        }
    }


    public synchronized void releaseCustConnection(Thread _thread) {
    	Object obj = dbConnectionMap.remove(_thread); //指定数据源存储对象!!
        if (obj == null) {
            ;
        } else {
        	try{
            Vector vector = new Vector();
            HashMap map = (HashMap) obj; //
            DBConnection[] conns=(DBConnection[]) (new ArrayList(map.values()).toArray(new DBConnection[0])); //
            for(int i=0;i<conns.length;i++){
            	if(!conns[i].isClosed()){
            		conns[i].close();
            	}
            }
        	}catch(Exception e){
        		;
        	}            
        }
    }

    public synchronized void releaseClientEnv(Thread _thread) {
        clientEnvMap.remove(_thread); //
    }
    
    /**
     * 清除线程对应的链接缓冲和客户端环境变量
     * @param _thread
     */
    public synchronized void releaseThreadInfo(Thread _thread,String ds){
    	HashMap map = (HashMap)dbConnectionMap.get(_thread); //指定数据源存储对象!!
        if (map != null) { //如果找到了该线程的对象!!
        	map.remove(ds);
	        if(map.size()==0){
	        	dbConnectionMap.remove(_thread); //
	        	clientEnvMap.remove(_thread); //
	        }
        }
    }

}
