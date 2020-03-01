package com.cool.dbaccess;

import java.sql.*;
import java.text.SimpleDateFormat;

import com.cool.common.logging.CoolLogger;



public abstract class AbstractDMO {
    
	//Fixed by James.W 2007.11.19  
    protected DBInitContext initContext = null; //
    protected int DS_RCONN_TIMES=10;
    protected long DS_RCONN_DELAY=-1l;

    public final DBConnection getConn(String _dsname) throws SQLException {
    	try{
        	return getConnection(_dsname);
    	}catch(SQLException e){
    		e.printStackTrace();
    		CoolLogger.getLogger(this).error("获取数据库链接错误：[DS="+_dsname+";ErrorCode="+e.getErrorCode()+"]"+e.getMessage());
    		throw new SQLException("不能连接到数据库！获取数据库链接错误：[DS="+_dsname+";ErrorCode="+e.getErrorCode()+"]"+e.getMessage());
    	}catch(Exception err){
    		CoolLogger.getLogger(this).error("发生不可控制异常！",err);
			throw new SQLException("发生不可控制异常！请查看日志。发生时间为【"+(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).format(new Date(System.currentTimeMillis()))+"】");
		}        
    }
    
    private final DBConnection getConnection(String _dsname) throws SQLException {

        if (initContext == null) {
            initContext = new DBInitContext();
        }
        return initContext.getConn(_dsname);
    }

    protected final DBConnection[] getAllConns() throws SQLException {
        if (initContext == null) {
            initContext = new DBInitContext();
        }
        return initContext.GetAllConns();
    }
}
