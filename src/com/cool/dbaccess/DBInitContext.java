package com.cool.dbaccess;

import java.sql.*;

import com.cool.common.system.CoolServerEnvironment;

/**
 * Nova环境上下文可以处理事务等...
 *
 */
public class DBInitContext {

    public DBInitContext() {
    }

    /**
     * 取得指定数据源,并参与同一事务!
     * @param _dsname
     * @return
     * @throws SQLException
     */
    public DBConnection getConn(String _dsname) throws SQLException {
        if (_dsname == null) {
            _dsname = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }

        return DBSessionFactory.getInstance().getConnection(Thread.currentThread(), _dsname); //取得当前线程的连接!!
    }
    
    /**
     * 是否获得数据库链接
     * @return
     * @deprecated 直接调用commit()、rollback()和release()
     */
    public boolean isGetConn() {
        return DBSessionFactory.getInstance().isGetConnection(Thread.currentThread());
    }

    /**
     * 得到线程所有用到的数据库链接
     * @return
     * @deprecated 直接调用commit()、rollback()和release()
     */
    public synchronized DBConnection[] GetAllConns() {
        return DBSessionFactory.getInstance().GetAllConnections(Thread.currentThread());
    }

    /**
     * 提交线程事务
     * @throws Exception
     */
    public synchronized int commit() throws Exception{
    	int rt=0;
    	DBSessionFactory nf=DBSessionFactory.getInstance();
    	Thread t=Thread.currentThread();
    	if(nf.isGetConnection(t)){
    		DBConnection[] conns=nf.GetAllConnections(t);
    		for(int i=0;i<conns.length;i++){
    			rt+=conns[i].getOpenStmtCount();    			
    			conns[i].transCommit();    			
    		}
    	}
    	return rt;
    }
    /**
     * 提交线程事务
     * @throws Exception
     */
    public synchronized int commit(String ds) throws Exception{
    	int rt=0;
    	
    	DBSessionFactory nf=DBSessionFactory.getInstance();
    	Thread t=Thread.currentThread();
    	if(nf.isGetConnection(t)){
    		DBConnection conns=nf.getConnection(t, ds);
    		rt+=conns.getOpenStmtCount();    			
    		conns.transCommit();
    	}
    	return rt;
    }
    /**
     * 回滚线程事务
     * @throws Exception
     */
    public synchronized int rollback() throws Exception{
    	int rt=0;
    	DBSessionFactory nf=DBSessionFactory.getInstance();
    	Thread t=Thread.currentThread();
    	if(nf.isGetConnection(t)){
    		DBConnection[] conns=nf.GetAllConnections(t);
    		for(int i=0;i<conns.length;i++){
    			rt+=conns[i].getOpenStmtCount();    			
    			conns[i].transRollback();    			
    		}
    	}
    	return rt;
    }
    /**
     * 回滚线程事务
     * @throws Exception
     */
    public synchronized int rollback(String ds) throws Exception{
    	int rt=0;
    	DBSessionFactory nf=DBSessionFactory.getInstance();
    	Thread t=Thread.currentThread();
    	if(nf.isGetConnection(t)){
    		DBConnection conns=nf.getConnection(t, ds);
    		rt+=conns.getOpenStmtCount();    			
    		conns.transRollback(); 
    	}
    	return rt;
    }

    /**
     * 清空线程缓冲信息
     */
    public synchronized void release() {
        try {
            DBSessionFactory.getInstance().releaseCustConnection(Thread.currentThread()); //
        } catch (Throwable th) {
        }

        try {
            DBSessionFactory.getInstance().releaseClientEnv(Thread.currentThread()); //
        } catch (Throwable th) {
        }
    }
    
    /**
     * 清空线程缓冲信息
     */
    public synchronized void release(String ds)throws Exception {
    	/**
    	 * 判断是否存在数据库链接，如存在则释放链接，然后释放线程所有缓冲信息。
    	 */
    	DBSessionFactory nf=DBSessionFactory.getInstance();
    	Thread t=Thread.currentThread();
    	if(nf.isGetConnection(t)){
    		DBConnection conns=nf.getConnection(t, ds);
    		conns.close();
    	}
    	DBSessionFactory.getInstance().releaseThreadInfo(Thread.currentThread(),ds);    	
    }
    
    public void commitAll() throws Exception {
        DBConnection[] conns = GetAllConns();
        if(conns==null)return;
        for (int i = 0; i < conns.length; i++) {
            conns[i].transCommit();
        }
    }

    public void rollbackAll() throws Exception {
        DBConnection[] conns = GetAllConns();
        if(conns==null)return;
        for (int i = 0; i < conns.length; i++) {
            conns[i].transRollback();
        }
    }

}
