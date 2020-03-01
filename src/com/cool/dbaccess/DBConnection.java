package com.cool.dbaccess;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;


import com.cool.common.system.CoolServerEnvironment;


public class DBConnection {

    private String dsName = null; //数据源名称!!

    private Connection conn = null;    
    private int openStmtCount = 0;

    /**
     * 默认数据源
     * @throws SQLException
     */
    public DBConnection() throws SQLException {
        this.dsName = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        createConn();
    }

    /**
     * 指定数据源
     * @param _dsname
     * @throws SQLException
     */
    public DBConnection(String _dsname) throws SQLException {
        this.dsName = _dsname;
        createConn();
    }

    private void createConn() throws SQLException { 
    	conn = DataSourceManager.getConnection(this.dsName);
    	//conn.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED); 
        conn.setAutoCommit(false);
    }
    

    public void transCommit() throws SQLException {
        conn.commit();
        this.openStmtCount=0;//提交后归零
    }

    public void transRollback() throws SQLException {
    	conn.rollback();  
    	this.openStmtCount=0;//提交后归零
    }

    public void close() throws SQLException {
    	conn.setAutoCommit(true);
    	conn.close();    	
    }
    /**
     * 是否关闭连接
     * @throws SQLException
     */
    public boolean isClosed() throws SQLException{
    	return conn.isClosed();
    }

    public java.sql.Connection getConn() {
        return conn;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return conn.getMetaData();
    }

    /**
     * 创建标准Statement
     * @return
     * @throws SQLException
     */
    public Statement createStatement() throws SQLException {
        openStmtCount = openStmtCount + 1;
        return conn.createStatement();
    }
    
    /**
     * 创建标准Statement，指定ResultSet类型和并发属性
     * @param resultSetType
     * @param resultSetConcurrency
     * @return
     * @throws SQLException
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        openStmtCount = openStmtCount + 1;
        return conn.createStatement(resultSetType, resultSetConcurrency);
    }

    /**
     * 创建PrepareStatement
     * @param sql
     * @return
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        openStmtCount = openStmtCount + 1;
        return conn.prepareStatement(sql);
    }
    
    /**
     * 创建PrepareStatement，指定ResultSet类型和并发属性
     * @param sql
     * @param resultSetType
     * @param resultSetConcurrency
     * @return
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String sql,int resultSetType, int resultSetConcurrency) throws SQLException{
    	openStmtCount = openStmtCount + 1;
        return conn.prepareStatement(sql,resultSetType,resultSetConcurrency);
    }

    /**
     * 创建CallableStatement,为存储过程与存储函数用!!!
     * @param sql
     * @return
     * @throws SQLException
     */
    public CallableStatement prepareCall(String sql) throws SQLException {
        openStmtCount = openStmtCount + 1;
        return conn.prepareCall(sql); //
    }

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    //打开游标的数量!!
    public int getOpenStmtCount() {
        return openStmtCount;
    }
}
