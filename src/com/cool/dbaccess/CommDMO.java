package com.cool.dbaccess;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.cool.common.constant.DMOConst;
import com.cool.common.logging.CoolLogger;
import com.cool.common.system.CoolServerEnvironment;
import com.cool.common.vo.HashVO;
import com.cool.common.vo.TableDataStruct;
import com.cool.dbaccess.adapter.DBInfoConst;
import com.cool.dbaccess.adapter.DatabaseAdapter;
import com.cool.dbaccess.ifc.PageableResultSet;



public class CommDMO extends AbstractDMO {
    private static Logger logger=CoolLogger.getLogger(CommDMO.class);
    private boolean isLog=false;

    public CommDMO() {
    }

    public CommDMO(boolean _isLog) {
        isLog=_isLog;
    }
    
    
    /**
     * 取得二维数组对象!!
     * @param ds
     * @param sql
     * @return
     * @throws Exception
     */
    public String[][] getStringArrayByDS(String ds, String sql) throws Exception {
    	return getStringArrayByDS(ds, sql, new Object[0]);
    }

    /**
     * 取得二维数组对象!!
     * @param ds
     * @param sql
     * @return
     * @throws Exception
     */
    public String[][] getStringArrayByDS(String ds, String sql, Object...parameters) throws Exception {
        DBConnection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String[][] str_data = null;

        long ll_1 = System.currentTimeMillis();
        try {

            if (ds == null) {
                ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
            }

            conn = getConn(ds); //
            stmt = conn.prepareStatement(sql);
            bindParameter(stmt, parameters);
            rs = stmt.executeQuery(); // 要出错就在这里!!!
            ResultSetMetaData rsmd = rs.getMetaData();
            int li_columncount = rsmd.getColumnCount();
            Vector vector = new Vector();
            while (rs.next()) {
                String[] str_row = new String[li_columncount];
                for (int i = 1; i <= li_columncount; i++) {
                    String str_cell = rs.getString(i);
                    if (str_cell == null) {
                        str_cell = "";
                    }
                    str_row[i - 1] = str_cell;
                }
                vector.add(str_row); //
            }

            int li_rowcount = vector.size(); //
            str_data = new String[li_rowcount][li_columncount];
            for (int i = 0; i < li_rowcount; i++) {
                String[] str_rowdata = (String[]) vector.get(i);
                str_data[i] = str_rowdata;
            }
            long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            
            if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
    			logger.debug("在数据源[" + ds + "]上执行SQL[" + getPreparedSqlInfo(sql, parameters) + "]耗时[" + ll_dealtime + "]");
    		}
            
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之[" + sql + "]");
            }
            
            return str_data;
        } catch (SQLException ex) {
        	logger.error("SQLException", ex);
            throw new SQLException("在数据源[" + ds + "]上执行SQL[" + sql + "]出错,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
        } catch (Exception ex) {
        	logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
            throw new Exception("在数据源[" + ds + "]上执行SQL[" + sql + "]出错,原因:" + ex.getMessage());
        } finally {
            try {if(rs!=null) rs.close(); } catch (Exception exx) { }
            try {if(stmt!=null) stmt.close(); } catch (Exception exx) { }
        }
    }
    
    /**
     * 取得表结构对象!!
     * @param ds
     * @param sql
     * @return
     * @throws Exception
     */
    public TableDataStruct getTableDataStructByDS(String ds, String sql) throws Exception {
    	return getTableDataStructByDS(ds, sql,new Object[0]);
    }

    /**
     * 取得表结构对象!!
     * @param ds
     * @param sql
     * @return
     * @throws Exception
     */
    public TableDataStruct getTableDataStructByDS(String ds, String sql, Object...parameters) throws Exception {
        DBConnection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Vector vector = new Vector();
        int li_columncount = 0;

        String str_fromtablename = null;
        String[] str_columnnames = null;
        String[] str_column_types = null;
        int[] int_precision = null;
        int[] int_scale = null;

        long ll_1 = System.currentTimeMillis();
        try {
            if (ds == null) {
                ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
            }

            conn = getConn(ds); //取得数据库连接!!
            stmt = conn.prepareStatement(sql);
            bindParameter(stmt, parameters);
            rs = stmt.executeQuery(); // 要出错就在这里!!!
            ResultSetMetaData rsmd = rs.getMetaData();
            li_columncount = rsmd.getColumnCount(); // 总共有几列
            str_columnnames = new String[li_columncount];
            str_column_types = new String[li_columncount];
            int_precision = new int[li_columncount];
            int_scale = new int[li_columncount];
            for (int i = 0; i < str_columnnames.length; i++) {
                if (i == 0) {
                    str_fromtablename = rsmd.getTableName(i + 1); //
                }
                str_columnnames[i] = rsmd.getColumnName(i + 1);
                str_column_types[i] = rsmd.getColumnTypeName(i + 1);
                int_precision[i] = rsmd.getPrecision(i+1);
                int_scale[i] = rsmd.getScale(i+1);
            }

            while (rs.next()) {
                String[] str_row = new String[li_columncount];
                for (int i = 1; i <= li_columncount; i++) {
                	String str_cell ="";
                	if (str_column_types[i - 1].equalsIgnoreCase("CLOB")) {//clob字段
	                	Clob clob = rs.getClob(i);
	                	if(clob != null)
	                		str_cell = clob.getSubString(1L, (int)clob.length());	
	                }else 
	                	str_cell = rs.getString(i);
                    if (str_cell == null) {
                        str_cell = "";
                    } else if (str_column_types[i - 1].equalsIgnoreCase("DATE")) {
                        str_cell = str_cell.substring(0, str_cell.length() - 2);
                    }
                    str_row[i - 1] = str_cell;
                }
                vector.add(str_row); //
            }
        } catch (SQLException ex) {
        	logger.error("SQLException", ex);
            throw new SQLException("在数据源[" + ds + "]上执行SQL[" + sql + "]出错,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
        } catch (Exception ex) {
        	logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
            throw new Exception("在数据源[" + ds + "]上执行SQL[" + sql + "]出错,原因:" + ex.getMessage());
        } finally {
            try {if(rs!=null) rs.close(); } catch (Exception ex) { }
            try {if(stmt!=null) stmt.close(); } catch (Exception ex) { }
        }

        int li_rowcount = vector.size(); //
        String[][] str_data = new String[li_rowcount][li_columncount];
        for (int i = 0; i < li_rowcount; i++) {
            String[] str_rowdata = (String[]) vector.get(i);
            str_data[i] = str_rowdata;
        }

        TableDataStruct struct = new TableDataStruct();
        struct.setFromtablename(str_fromtablename); // 设置从哪个表取数!!!!!
        struct.setTable_header(str_columnnames); // 设置表头结构
        struct.setTable_body(str_data); // 设置表体结构
        struct.setTable_body_type(str_column_types); // 设置字段类型
        struct.setTable_body_type_precision(int_precision);//设置指定列的指定列宽
        struct.setTable_body_type_scale(int_scale);//设置指定列的小数点右边的位数

        long ll_2 = System.currentTimeMillis();
        long ll_dealtime = ll_2 - ll_1;
        
        if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
			logger.debug("在数据源[" + ds + "]上执行SQL[" + getPreparedSqlInfo(sql, parameters) + "]耗时[" + ll_dealtime + "]");
		}
        
        if (ll_dealtime > getWarnSQLTime()) {
            logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之[" + sql + "]");
        }
        
        return struct;
    }
    
    private HashVO[] getHashVoArrayByConn(DBConnection conn, String ds,boolean isLimitRows,String sql, Object...parameters) throws Exception {
    	PreparedStatement stmt = null;
        ResultSet rs = null;
        int limitRows = 500000;//默认50万条数据
        if( isLimitRows ){
        	limitRows = 50000;//限定为5000条以内
        }
        
        long ll_1 = System.currentTimeMillis();
    	try{
	        Vector<HashVO> vResult = new Vector<HashVO>();
	    	stmt = conn.prepareStatement(sql);
	        bindParameter(stmt, parameters);
	        rs = stmt.executeQuery(); // 要出错就在这里!!!
	        ResultSetMetaData rsmd = rs.getMetaData();
	        HashVO voTmp = null;
	        int rows = 0;
	        while (rs.next()) {
	            voTmp = new HashVO();
	            for (int i = 1; i < rsmd.getColumnCount() + 1; i++) {
	                String str_colname = rsmd.getColumnName(i); // 列名
	                int li_coltype = rsmd.getColumnType(i); // 列类型
	                String str_tablename = rsmd.getTableName(i); //
	                Object value = null; //
	                if (li_coltype == Types.VARCHAR) { // 如果是字符
	                    value = rs.getString(i);
	                } else if (li_coltype == Types.NUMERIC) { // 如果是Number
	                    value = rs.getBigDecimal(i); //
	                } else if (li_coltype == Types.DATE || li_coltype == Types.TIMESTAMP) { // 如果是日期或时间类型,统统精确到秒,Oracle中的Date类型是Types.DATE,但返回的值是Timestamp!!!
	                    value = rs.getTimestamp(i);
	                } else if (li_coltype == Types.SMALLINT) { // 如果是整数
	                	if(rs.getBigDecimal(i) != null)
	                		value = new Integer(rs.getBigDecimal(i).intValue()); //
	                } else if (li_coltype == Types.INTEGER) { // 如果是整数
	                	if(rs.getBigDecimal(i) != null)
	                		value = new Long(rs.getBigDecimal(i).longValue()); //
	                } else if (li_coltype == Types.DECIMAL || li_coltype == Types.DOUBLE || li_coltype == Types.DOUBLE ||
	                           li_coltype == Types.FLOAT) {
	                    value = rs.getBigDecimal(i); //
	                } else if (li_coltype == Types.CLOB) {//clob字段
	                	Clob clob = rs.getClob(i);
	                	if(clob != null)
	                		value = clob.getSubString(1L, (int)clob.length());	
	                } else if(li_coltype == Types.BLOB){
	                	Blob blob = rs.getBlob(i);
	                	if(blob != null)
	                		value = blob.getBytes(1L,(int)blob.length());
	                }
	                else {
	                    value = rs.getObject(i);
	                }
	                voTmp.setColumnType(str_colname, li_coltype); //设置字段数据类型
	                voTmp.setAttributeValue(str_colname, value); // 设置值!!
	                voTmp.setTableName(str_colname, str_tablename); // 设置表名
	                
	            }
	            vResult.add(voTmp); //
	            //超过限定行数的数据不取，防止服务器崩溃
	            rows ++;
	            if( rows == limitRows )
	            	break;
	        }
	        
	        if( rows == limitRows && rs.next() ){
	        	logger.warn("警告:在数据源[" + ds + "]上执行SQL[" + sql + "]查询结果超过[" + rows + "]条,属于大数据量查询，可能需要优化！");
	        }
	       
	        
	        HashVO[] vos = new HashVO[vResult.size()];
	        vResult.copyInto(vos);
	        long ll_2 = System.currentTimeMillis();
	        long ll_dealtime = ll_2 - ll_1;
	         
	        if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
	 			logger.debug("在数据源[" + ds + "]上执行SQL[" + getPreparedSqlInfo(sql, parameters) + "]耗时[" + ll_dealtime + "],返回结果数[" + rows + "]条.");
	 		}
	         
	        if (ll_dealtime > getWarnSQLTime()) {
	            logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之[" + getPreparedSqlInfo(sql, parameters) + "]");
	        }
	        return vos;
    	}
    	catch (SQLException ex) {
    		logger.error("SQLException", ex);
            throw new SQLException("在数据源[" + ds + "]上执行SQL[" + getPreparedSqlInfo(sql, parameters) + "]出错,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
        } catch (Exception ex) {
        	logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
            throw new Exception("在数据源[" + ds + "]上执行SQL[" + getPreparedSqlInfo(sql, parameters) + "]出错,原因:" + ex.getMessage());
        } 
    	finally{
    		 try {if(rs!=null) rs.close(); } catch (Exception exx) { }
             try {if(stmt!=null) stmt.close(); } catch (Exception exx) { }
    	}
    	 
    }
    /**
     * 根据SQL语句返回HashVO
     * @param ds
     * @param sql
     * @return
     * @throws Exception
     * @throws SQLException
     */
    public HashVO[] getHashVoArrayByDS(String ds, String sql, Object...parameters) throws Exception {
        if (ds == null) {
           ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }

        DBConnection conn = getConn(ds); //
        HashVO[] vos = getHashVoArrayByConn(conn, ds, true, sql, parameters); 

        return vos;
    }
    
    /**
     * 根据SQL语句返回HASHVO，不限定返回的行数大小
     * @param ds
     * @param sql
     * @param parameters
     * @return
     * @throws Exception
     */
    public HashVO[] getHashVoArrayByDSUnlimitRows(String ds, String sql, Object...parameters) throws Exception {
        if (ds == null) {
            ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
         }

         DBConnection conn = getConn(ds); //
         HashVO[] vos = getHashVoArrayByConn(conn, ds, false, sql, parameters); 

         return vos;
     }
    
    /**
     * 根据SQL语句返回HashVO,不限定行数
     * @param ds
     * @param sql
     * @return
     * @throws Exception
     * @throws SQLException
     */
    public HashVO[] getHashVoArrayByDSUnlimitRows(String ds, String sql) throws Exception {
        return getHashVoArrayByDSUnlimitRows(ds, sql, new Object[0]);
    }

    /**
     * 根据SQL语句返回HashVO
     * @param ds
     * @param sql
     * @return
     * @throws Exception
     * @throws SQLException
     */
    public HashVO[] getHashVoArrayByDS(String ds, String sql) throws Exception {
        return getHashVoArrayByDS(ds, sql, new Object[0]);
    }
    
    /**
     * 根据SQL语句返回HashVO（立即释放连接）
     * @param ds
     * @param sql
     * @return
     * @throws Exception
     * @throws SQLException
     */
    public HashVO[] getHashVoAtOnceByDS(String ds, String sql) throws Exception {
    	return getHashVoAtOnceByDS(ds, sql, new Object[0]);
    }
    
    /**
     * 根据SQL语句返回HashVO（立即释放连接）
     * @param ds
     * @param sql
     * @return
     * @throws Exception
     * @throws SQLException
     */
    public HashVO[] getHashVoAtOnceByDS(String ds, String sql,
			Object... parameters) throws Exception {
		DBConnection conn = null;
		try {
			if (ds == null) {
				ds = CoolServerEnvironment.getInstance()
						.getDefaultDataSourceName();
			}

			conn = getConn(ds); //
			HashVO[] vos = getHashVoArrayByConn(conn, ds,true, sql, parameters);

			return vos;
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (Exception ex) {
			}
		}

	}
    
    /**
     * 根据SQL获取总记录数
     * @param ds
     * @param sql
     * @return
     * @throws Exception
     */
	public int getRowsCountByDS(String ds, String sql) throws Exception {
		int rowsCount = 0;// 默认0条
		String querySql = "select count(1) rowsCount from (" + sql + ")";

		HashVO[] vos = getHashVoArrayByDS(ds, querySql);
		if (vos != null && vos.length > 0) {
			rowsCount = vos[0].getIntegerValue(0);
		}

		return rowsCount;
	}
    
    /**
     * 根据分页查询数据，只返回一页数据
     * @param dataSource
     * @param sql
     * @param pageNum
     * @param rowCountPerPage
     * @return
     * @throws Exception
     */
    public Map<String, Object> getHashVoArrayByPage(String dataSource,String sql,int pageNum, int rowCountPerPage) throws Exception {
    	//分为两步，先计算出记录总数，再取分页数据
    	int rowsCount = getRowsCountByDS(dataSource,sql);
    	Map<String, Object> result = getHashVoArrayByPage(dataSource, sql, pageNum, rowCountPerPage, rowsCount);
    		
    	return result;
    }
    
    /**
     * 根据分页查询数据，只返回一页数据(已知总记录数)
     * @param dataSource
     * @param sql
     * @param pageNum
     * @param rowCountPerPage
     * @param totalRows 总记录数
     * @return
     * @throws Exception
     */
    public Map<String, Object> getHashVoArrayByPage(String dataSource,String sql,int pageNum,int rowCountPerPage,int totalRows) throws Exception {
    	PreparedStatement stmt = null;
    	PageableResultSet pageRs = null;
    	Map<String, Object> result = new HashMap<String, Object>();
      
        long ll_1 = System.currentTimeMillis();
    	try{
    		if (dataSource == null) {
    			dataSource = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
    		}
    		String querySQL = sql;
    		
    		String dsUrl = DataSourceManager.getDataSourceUrl(dataSource);
    		if(dsUrl != null && dsUrl.startsWith("jdbc:oracle")){
    			int startNum = (pageNum-1)*rowCountPerPage;
    			int endNum = pageNum*rowCountPerPage;
    			querySQL = "select * from (select rt.*,rownum rn from ("+sql+") rt where  rownum <="+endNum+") where rn >"+startNum;
    			//每次只取了当前页的数据,所以对于ResultSet游标都要指向1
    			pageNum = 1;
    		}
    		
    		DBConnection conn = getConn(dataSource);
    		
	        Vector<HashVO> vResult = new Vector<HashVO>();
	    	stmt = conn.prepareStatement(querySQL,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);

	    	pageRs = new PageableResultSet(stmt.executeQuery(),totalRows); // 要出错就在这里!!!
	        ResultSetMetaData rsmd = pageRs.getMetaData();
	        
	        //计算要取的数据,定位到pageNum页
	        pageRs.setPageSize(rowCountPerPage);//设置页大小
	        pageRs.gotoPage(pageNum);
	        
	        HashVO voTmp = null;
	        for( int rowNum=0; rowNum < pageRs.getPageRowsCount(); rowNum++ ) {
	            voTmp = new HashVO();
	            for (int i = 1; i < rsmd.getColumnCount() + 1; i++) {
	                String str_colname = rsmd.getColumnName(i); // 列名
	                int li_coltype = rsmd.getColumnType(i); // 列类型
	                String str_tablename = rsmd.getTableName(i); //
	                Object value = null; //
	                if (li_coltype == Types.VARCHAR) { // 如果是字符
	                    value = pageRs.getString(i);
	                } else if (li_coltype == Types.NUMERIC) { // 如果是Number
	                    value = pageRs.getBigDecimal(i); //
	                } else if (li_coltype == Types.DATE || li_coltype == Types.TIMESTAMP) { // 如果是日期或时间类型,统统精确到秒,Oracle中的Date类型是Types.DATE,但返回的值是Timestamp!!!
	                    value = pageRs.getTimestamp(i);
	                } else if (li_coltype == Types.SMALLINT) { // 如果是整数
	                    value = new Integer(pageRs.getBigDecimal(i).intValue()); //
	                } else if (li_coltype == Types.INTEGER) { // 如果是整数
	                    value = new Long(pageRs.getBigDecimal(i).longValue()); //
	                } else if (li_coltype == Types.DECIMAL || li_coltype == Types.DOUBLE || li_coltype == Types.DOUBLE ||
	                           li_coltype == Types.FLOAT) {
	                    value = pageRs.getBigDecimal(i); //
	                } else if (li_coltype == Types.CLOB) {//clob字段
	                	Clob clob = pageRs.getClob(i);
	                	if(clob != null)
	                		value = clob.getSubString(1L, (int)clob.length());	
	                } else if(li_coltype == Types.BLOB){
	                	Blob blob = pageRs.getBlob(i);
	                	if(blob != null)
	                		value = blob.getBytes(1L,(int)blob.length());
	                }
	                else {
	                    value = pageRs.getObject(i);
	                }
	                voTmp.setColumnType(str_colname, li_coltype); //设置字段数据类型
	                voTmp.setAttributeValue(str_colname, value); // 设置值!!
	                voTmp.setTableName(str_colname, str_tablename); // 设置表名
	                
	            }
	            vResult.add(voTmp); //
	            
	            if(!pageRs.next()){//游标向下移动一个
	            	break;
	            }
	        }	        
	        
	        HashVO[] vos = new HashVO[vResult.size()];
	        vResult.copyInto(vos);
	        long ll_2 = System.currentTimeMillis();
	        long ll_dealtime = ll_2 - ll_1;
	         
	        if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
	 			logger.debug("在数据源[" + dataSource + "]上执行SQL[" + sql + "]耗时[" + ll_dealtime + "],查询第" + pageNum + "页，返回结果数[" + pageRs.getPageRowsCount() + "]条.");
	 		}
	         
	        if (ll_dealtime > getWarnSQLTime()) {
	            logger.warn("警告:在数据源[" + dataSource + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之[" + sql + "]");
	        }
	        
	        result.put(DMOConst.ROWCOUNT, pageRs.getRowsCount());//总记录数
	        result.put(DMOConst.HASHVOARRAY, vos);
	        
	        return result;
    	}
    	catch (SQLException ex) {
    		logger.error("SQLException", ex);
            throw new SQLException("在数据源[" + dataSource + "]上执行SQL[" + sql + "]出错,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
        } catch (Exception ex) {
        	logger.error("在数据源[" + dataSource + "]上执行SQL[" + sql + "]出错！", ex);
            throw new Exception("在数据源[" + dataSource + "]上执行SQL[" + sql + "]出错,原因:" + ex.getMessage());
        } 
    	finally{
    		 try {if(pageRs!=null) pageRs.close(); } catch (Exception exx) { }
             try {if(stmt!=null) stmt.close(); } catch (Exception exx) { }
    	}
    	 
    }
    
    
    /**
     * 执行一条SQL!
     * @param _datasourcename
     * @param _sql
     * @return int 操作涉及行数
     * @throws Exception
     */
    public int executeUpdateByDS(String ds, String sql, Object...parameters) throws Exception {
    	 DBConnection conn = null;
         PreparedStatement p_stmt = null; //
         
         
         try {
             if (ds == null) {
                 ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
             }
             conn = getConn(ds);
             
             long ll_1 = System.currentTimeMillis();            
             p_stmt = conn.prepareStatement(sql); 
             bindParameter(p_stmt, parameters);
             int rows=p_stmt.executeUpdate(); 
             writeLog(sql);
             long ll_2 = System.currentTimeMillis();
             long ll_dealtime = ll_2 - ll_1;
             
             if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
     			logger.debug("在数据源[" + ds + "]上执行SQL[" + getPreparedSqlInfo(sql, parameters) + "]耗时[" + ll_dealtime + "]");
     		 }
             
             if (ll_dealtime > getWarnSQLTime()) {
                 logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之[" + sql + "]");
             }
             return rows;
         } catch (SQLException ex) {
        	 logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
             Vector errInd = DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getErrDesc(ex, conn);
             if (errInd == null) {
            	 
                 throw new SQLException("在数据源[" + ds + "]上执行SQL[" + sql + "]出错,错误编码[" + ex.getErrorCode() +
                                    "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
             } else{
                 throw DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getDBErrExplainer().resolve(errInd);
             }
         } catch (Exception ex) {
        	 logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
             throw new Exception("在数据源[" + ds + "]执行SQL错误:" + sql + ",原因:" + ex.getMessage());
         } finally {
             try {if (p_stmt != null) p_stmt.close(); } catch (Exception ex) { }
         }
    }

    /**
     * 执行一条SQL!
     * @param _datasourcename
     * @param _sql
     * @return int 操作涉及行数
     * @throws Exception
     */
    public int executeUpdateByDS(String ds, String sql) throws Exception {
       return executeUpdateByDS(ds,sql,new Object[0]);
    }
    
    /**
     * 执行一条sql并提交
     * @param ds 数据源名称
     * @param sql 操作的sql
     * @return int 操作涉及行数
     * @throws Exception
     */
    public int execAtOnceByDS(String ds, String sql) throws Exception {
    	return execAtOnceByDS(ds, sql, new Object[0]);
    }
    
    
    /**
     * 执行一条sql并提交
     * @param ds 数据源名称
     * @param sql 操作的sql
     * @return int 操作涉及行数
     * @throws Exception
     */
    public int execAtOnceByDS(String ds, String sql, Object...parameters) throws Exception {
        DBConnection conn = null;
        PreparedStatement p_stmt = null; 
        try {
            ds = (ds==null)?CoolServerEnvironment.getInstance().getDefaultDataSourceName():ds;
            conn = new DBConnection(ds);
            
            long ll_1 = System.currentTimeMillis();            
            p_stmt = conn.prepareStatement(sql); 
            bindParameter(p_stmt, parameters);
            int rows=p_stmt.executeUpdate(); 
            writeLog(sql);
            conn.transCommit();
            long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            
            if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
     			logger.debug("在数据源[" + ds + "]上执行SQL[" + getPreparedSqlInfo(sql, parameters) + "]耗时[" + ll_dealtime + "]");
     		 }
            
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之[" + sql + "]");
            }
           
            return rows;
        } catch (SQLException ex) {
        	try {conn.transRollback(); } catch (Exception e) { }
        	throw new SQLException("在数据源[" + ds + "]上执行SQL[" + sql + "]出错,错误编码[" + ex.getErrorCode() +
                    "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());            
        } catch (Exception ex) {
        	logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
        	try {conn.transRollback(); } catch (Exception e) { }
            throw new Exception("在数据源[" + ds + "]执行SQL错误:" + sql + ",原因:" + ex.getMessage());
        } finally {
            try {if (p_stmt != null) p_stmt.close(); } catch (Exception ex) { }
            try {if (conn != null) conn.close(); } catch (Exception ex) { }
        }
    }
    
    /**
     * 批量执行sql
     * @param ds
     * @param sqls
     * @param keys
     * @return
     * @throws Exception
     */
    public HashMap getHashVoArrayReturnMapByDS(String ds, String[] sqls, String[] keys) throws Exception {
        if (ds == null) {
            ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }
        //James.W add 2007.11.16
        DBConnection conn = getConn(ds);

        HashMap map = new HashMap(); //
        for (int i = 0; i < sqls.length; i++) {
            HashVO[] hvs = getHashVoArrayByDS(ds,sqls[i],conn);
            map.put(keys[i], hvs); //
        }
        return map; //
    }
    
    /**
     * 批量执行sql
     * @param _datasourcename
     * @param _sqls
     * @return
     * @throws Exception
     */
    public Vector getHashVoArrayReturnVectorByDS(String _datasourcename, String[] _sqls) throws Exception {
        if (_datasourcename == null) {
            _datasourcename = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }
        
        //James.W add 2007.11.16
        DBConnection conn = getConn(_datasourcename); //
        

        Vector vector = new Vector(); //
        for (int i = 0; i < _sqls.length; i++) {
            HashVO[] hvs = getHashVoArrayByDS(_datasourcename,_sqls[i],conn);
            vector.add(hvs); //
        }
        return vector;
    }
    
    /**
     * 执行一个批量导入记录的操作，批量提交
     * @param fromdriver 来源数据库驱动
     * @param fromurl 来源数据库url
     * @param fromuid 来源数据库用户
     * @param fromupwd 来源数据库密码
     * @param fromsql 来源sql
     * @param fromcols 来源字段列号序列
     * @param tods 导入数据源
     * @param tosql 导入sql
     * @param batch 批量大小
     * @throws Exception
     */
    public void executeImportByDS(String fromdriver, String fromurl, String fromuid, String fromupwd, String fromsql, int[] fromcols,
    		                      String tods, String tosql, int batch) throws Exception {
        Connection fromconn = null;
        PreparedStatement fromps = null; //
        
        if (tods == null) {
            tods = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }
        try {
        	long ll_1 = System.currentTimeMillis();   
        	Class.forName(fromdriver);
			fromconn = DriverManager.getConnection(fromurl, fromuid, fromupwd);
			fromps = fromconn.prepareStatement(fromsql); 
            ResultSet fromrs=fromps.executeQuery();
            fromrs.setFetchSize(100);
            
            long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + tods + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之[" + fromsql + "]");
            }
            executeImportByDS(tods, tosql, fromrs, fromcols, batch);            
        } catch (SQLException ex) {
            throw new SQLException("在数据源[" + fromurl + "]上执行SQL[" + fromsql + "]出错,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());            
        } catch (Exception ex) {
        	logger.error("在数据源[" + fromurl + "]上执行SQL[" + fromsql + "]出错！", ex);
            throw new Exception("在数据源[" + fromurl + "]执行SQL错误:" + fromsql + ",原因:" + ex.getMessage());
        } finally {
            try {if (fromps != null) fromps.close(); } catch (Exception ex) { }
        }
    }
    
    
    /**
     * 执行一个批量导入记录的操作，批量提交
     * @param fromds 来源数据源
     * @param fromsql 来源sql
     * @param fromcols 来源字段列号序列
     * @param tods 导入数据源
     * @param tosql 导入sql
     * @param batch 批量大小
     * @throws Exception
     */
    public long executeImportByDS(String fromds, String fromsql, int[] fromcols,String tods, String tosql, int batch,boolean isIgnoreException) throws Exception {
        DBConnection fromconn = null;
        PreparedStatement fromps = null; //
        if (fromds == null) {
            fromds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }
        if (tods == null) {
            tods = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }
        logger.info("在数据源[" + fromds + "]上执行批量导入查询SQL[" + fromsql + "]");
        try {
        	long ll_1 = System.currentTimeMillis();   
            fromconn = getConn(fromds);
            fromps = fromconn.prepareStatement(fromsql); 
            ResultSet fromrs=fromps.executeQuery();
            fromrs.setFetchSize(100);
            
            long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + tods + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之[" + fromsql + "]");
            }
           return  executeImportByDS(tods, tosql, fromrs, fromcols, batch,isIgnoreException);
        } catch (Exception ex) {
        	logger.error("执行数据间数据批量导入失败!", ex);
            throw new Exception("执行数据间数据批量导入失败!" + ex.getMessage());
        } finally {
            try {if (fromps != null) fromps.close(); } catch (Exception ex) { }
        }
        
    }
    
    /**
     * 执行一个批量导入记录的操作，批量提交
     * @param fromds 来源数据源
     * @param fromsql 来源sql
     * @param fromcols 来源字段列号序列
     * @param tods 导入数据源
     * @param tosql 导入sql
     * @param batch 批量大小
     * @throws Exception
     */
    public long executeImportByDS(String fromds, String fromsql, int[] fromcols,String tods, String tosql, int batch) throws Exception {
    	return executeImportByDS(fromds,fromsql,fromcols,tods,tosql,batch,false);
    }
    
    /**
     * 执行一个批量导入记录的操作，批量提交（不忽略异常情况）
     * @param _datasourcename
     * @param _sql 带问号的sql语句
     * @param rs 来源数据集
     * @param cols 需要插入的数据列号设置（1开始排序）
     * @param batch 批量提交大小
     * @throws Exception
     * @return 返回写入的行数
     */
    public long executeImportByDS(String ds, String sql, ResultSet rs, int[] cols, int batch) throws Exception {
    	return executeImportByDS(ds,sql,rs,cols,batch,false);
    }
    
    /**
     * 执行一个批量导入记录的操作，批量提交
     * @param _datasourcename
     * @param _sql 带问号的sql语句
     * @param rs 来源数据集
     * @param cols 需要插入的数据列号设置（1开始排序）
     * @param batch 批量提交大小
     * @throws Exception
     * @return 返回写入的行数
     */
    public long executeImportByDS(String ds, String sql, ResultSet rs, int[] cols, int batch,boolean isIgnoreException) throws Exception {
        DBConnection conn = null;
        PreparedStatement ps = null; //
        if (ds == null) {
            ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }
        logger.info("在数据源[" + ds + "]批量【"+batch+"】数据插入SQL语句： 【"+sql+"】");
        
        if(rs==null) return 0;
        
        try {
            conn = getConn(ds);
            
            long ll_1 = System.currentTimeMillis();            
            ps = conn.prepareStatement(sql); 
            
            ResultSetMetaData rsmd= rs.getMetaData();
            
            long row=0;
            while(rs.next()){
            	row++;
            	for(int i=0;i<cols.length;i++){
            		int colType = rsmd.getColumnType(cols[i]);
            		if(colType == Types.DATE || colType == Types.TIMESTAMP){//日期类型
        				Timestamp timestamp = rs.getTimestamp(cols[i]);
        				ps.setTimestamp(i+1, timestamp);
            			
            		}else{
            			ps.setObject(i+1, rs.getObject(cols[i]), colType); 
            		}
            	}
            	ps.addBatch();
            	if(row%batch==0) {
            		if(isIgnoreException){
            			try{
            				ps.executeBatch();
            			}catch(SQLException ex){
            				continue;
            			}
            		}else
            			ps.executeBatch();
            		
            		this.commit(ds);
            		
            		logger.debug("已完成【"+row+"】条数据插入，SQL语句： 【"+sql+"】。");
            	}
            }
            
           	ps.executeBatch();
            this.commit(ds);
            logger.debug("已全部完成【"+row+"】条数据插入，SQL语句： 【"+sql+"】。");
            
            writeLog(sql);
            long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之[" + sql + "]");
            }
            return row;
        } catch (SQLException ex) {
        	logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
            Vector errInd = DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getErrDesc(ex, conn);
            if (errInd == null) {
                throw new SQLException("在数据源[" + ds + "]上执行SQL[" + sql + "]出错,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
            } else{
                throw DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getDBErrExplainer().resolve(errInd);
            }
        } catch (Exception ex) {
        	logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
            throw new Exception("在数据源[" + ds + "]执行SQL错误:" + sql + ",原因:" + ex.getMessage());
        } finally {
            try {if (ps != null) ps.close(); } catch (Exception ex) { }
        }
    }
    
    /**
     * 批量执行sql
     * @param ds
     * @param sqls
     * @return int[] 对应每条Sql执行的涉及行数
     * @throws Exception
     */
    public int[] executeBatchByDS(String ds, String[] sqls) throws Exception {
        if (ds == null) {
            ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }
        java.util.List list = Arrays.asList(sqls); //转换成一个List
        int[] rt=executeBatchByDS(ds, list); //
        return rt;
    }

    /**
     * 批量执行Sql
     * @param ds
     * @param lst
     * @return int[] 对应每条Sql执行所涉及的行数
     * @throws Exception
     */
    public int[] executeBatchByDS(String ds, java.util.List lst) throws Exception {
        DBConnection conn = null;
        Statement p_stmt = null;
        
        long ll_1 = System.currentTimeMillis();
        try {
            if (ds == null) {
                ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
            }
            conn = getConn(ds); //
            p_stmt = conn.createStatement(); //
            for (int i = 0; i < lst.size(); i++) {
                String sql = (String) lst.get(i);
                p_stmt.addBatch(sql); //批增加!!!
                //writeLog(sql); zhangzy:日志太多了，受不了！                
            }
            int[] rt = p_stmt.executeBatch();
            
            long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            
            if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
     			logger.debug("在数据源[" + ds + "]上执行批量SQL耗时[" + ll_dealtime + "]");
     		 }
            
            
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之["+lst.size()+"条sql语句]");
            }
            
            return rt;
        } catch (SQLException ex) {
        	logger.error("在数据源[" + ds + "]上执行执行批量SQL失败！", ex);
            Vector errInd = DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getErrDesc(ex, conn);
            if (errInd == null) {
                String str_message = "";
                for (int i = 0; i < lst.size(); i++) {
                    str_message = str_message + lst.get(i) + "\n"; //
                }
                throw new SQLException("在数据源[" + ds + "]上执行批量SQL失败:\n" + str_message + ",错误编码[" +
                                       ex.getErrorCode() + "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());

            } else
                throw DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getDBErrExplainer().resolve(errInd);
        } catch (Exception ex) {
            String str_message = "";
            for (int i = 0; i < lst.size(); i++) {
                str_message = str_message + lst.get(i) + "\n"; //
            }
            logger.error("在数据源[" + ds + "]上执行批量SQL失败！", ex);
            throw new Exception("在数据源[" + ds + "]上执行批量SQL[" + str_message + "]失败,原因:[" + ex.getMessage() + "]");
        } finally {
            try {
                if (p_stmt != null) {
                    p_stmt.close();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    
    /**
     * 更新Blob字段（目前仅仅在Oracle上测试）
     * 使用条件：blob字段必须初始化过（EMPTY_BLOB()或者已经有值）
     * 
     * 如果表有唯一主键，但不能确定是否初始化过blob字段，那么不要调用本方法。
     * @param _datasourcename
     * @param _sql 注意形式一定如select blob_field from blob_table where key=xxx for update 形式。
     * @param data 需要保存的内容的二进制序列
     * @throws Exception
     */
    public void executeUpdateBlobByDS(String ds, String sql, byte[] data) throws Exception {
        if(data==null||data.length==0||sql==null||sql.trim().equals("")){
        	throw new Exception("在数据源[" + ds + "]执行SQL错误:" + sql + ",原因:提供信息不全！");
        }
    	
    	DBConnection conn = null;
        PreparedStatement p_stmt = null; //
        ResultSet rs=null;

        long ll_1 = System.currentTimeMillis();
        try {
            if (ds == null) {
                ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
            }
            conn = new DBInitContext().getConn(ds); // 取得数据库连接!
            p_stmt = conn.prepareStatement(sql); // 创建游标,凡是一次远程调用都是在同一事务中进行,即取得的是同一个Connection!!
            rs=p_stmt.executeQuery();
			if(rs.next()){
				oracle.sql.BLOB blob = (oracle.sql.BLOB) rs.getBlob(1);
				OutputStream out=blob.getBinaryOutputStream();
				out.write(data, 0, data.length);
				out.flush();
				out.close();
			}
			    

            long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
     			logger.debug("在数据源[" + ds + "]上执行SQL[" + sql + "]耗时[" + ll_dealtime + "]");
     		 }
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之["+sql+"]");
            }
            writeLog(sql); 
        } catch (SQLException ex) {
        	logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
            Vector errInd = DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getErrDesc(ex, conn);
            if (errInd == null) {
                throw new SQLException("在数据源[" + ds + "]上执行SQL[" + sql + "]出错,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
            } else
                throw DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getDBErrExplainer().resolve(errInd);
        } catch (Exception ex) {
        	logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
            throw new Exception("在数据源[" + ds + "]执行SQL错误:" + sql + ",原因:" + ex.getMessage());
        } finally {
        	try {
                if (rs != null) {
                    rs.close(); 
                }
            } catch (Exception ex) {
            }
            try {
                if (p_stmt != null) {
                    p_stmt.close(); 
                }
            } catch (Exception ex) {
            }
        }
    }//T针对Clob字段处理大同小异，但对input/output Stream稍有不同
    
    
    /**
     * 修改clob字段
     * @param ds 数据源
     * @param clobColumnName clob字段名
     * @param tableName 表名
     * @param whereCondition 查询条件(形如id=1)
     * @param data 赋值数据
     * @throws Exception
     */
    public void executeUpdateClobByDS(String ds, String clobColumnName, String tableName, String whereCondition, String data) throws Exception {
        if(data==null||clobColumnName==null||tableName==null){
        	throw new Exception("在数据源[" + ds + "]更新clob失败, 原因:提供信息不全！");
        }
    	
    	DBConnection conn = null;
        PreparedStatement p_stmt = null; //
        ResultSet rs=null;
        String sql = null;
        long ll_1 = System.currentTimeMillis();
        try {
            if (ds == null) {
                ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
            }
            //先清空clob
            sql = "update "+tableName+" set "+clobColumnName+"=empty_clob()";
            if(whereCondition != null)
            	sql += " where "+whereCondition;
            executeUpdateByDS(ds, sql);
            
            conn = new DBInitContext().getConn(ds); // 取得数据库连接!
//            sql = "select "+clobColumnName+" from "+tableName;
//            if(whereCondition != null)
//            	sql += " where "+whereCondition;
//            sql += " for update";
            
            sql = "update "+tableName+" set "+clobColumnName+"=?";
            if(whereCondition != null && !whereCondition.trim().equals("")){
            	sql += " where "+whereCondition;
            }
            
            p_stmt = conn.prepareStatement(sql); // 创建游标,凡是一次远程调用都是在同一事务中进行,即取得的是同一个Connection!!
            BufferedReader reader = new BufferedReader(new StringReader(data));
            p_stmt.setCharacterStream(1,reader,data.length());
//            p_stmt.setClob(1, reader, data.length());
            p_stmt.executeUpdate();
            reader.close();
//            rs=p_stmt.executeQuery();
//			if(rs.next()){
//				Clob clob = rs.getClob(1);
//				BufferedWriter out = new BufferedWriter(clob.getCharacterOutputStream());
//				BufferedReader in = new BufferedReader(new StringReader(data));
//
//				int c;
//
//				while ((c=in.read())!=-1) {
//
//					out.write(c);
//				}
//				in.close();
//				out.flush();
//				out.close();
//			}
			    

            long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
     			logger.debug("在数据源[" + ds + "]上执行SQL[" + sql + "]耗时[" + ll_dealtime + "]");
     		 }
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之["+sql+"]");
            }
            writeLog(sql); 
        } catch (SQLException ex) {
        	logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
            Vector errInd = DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getErrDesc(ex, conn);
            if (errInd == null) {
                throw new SQLException("在数据源[" + ds + "]上执行SQL[" + sql + "]出错,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
            } else
                throw DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getDBErrExplainer().resolve(errInd);
        } catch (Exception ex) {
        	logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
            throw new Exception("在数据源[" + ds + "]执行SQL错误:" + sql + ",原因:" + ex.getMessage());
        } finally {
        	try {
                if (rs != null) {
                    rs.close(); 
                }
            } catch (Exception ex) {
            }
            try {
                if (p_stmt != null) {
                    p_stmt.close(); 
                }
            } catch (Exception ex) {
            }
        }
    }
    
    
    /**
     * 更新Blob字段（目前仅仅在Oracle上测试）
     * @param ds
     * @param tblname
     * @param tblkey
     * @param keyvalue
     * @param blobfield
     * @param data
     * @throws Exception
     */
    public void executeUpdateBlobByDS(String ds, String tblname, String tblkey, String keyvalue, String blobfield, byte[] data) throws Exception {
        if(data==null||data.length==0
        		||tblname==null||tblname.trim().equals("")
        		||tblkey==null||tblkey.trim().equals("")
        		||keyvalue==null||keyvalue.trim().equals("")
        		){
        	throw new Exception("在数据源[" + ds + "]执行对表【"+tblname+"】中主键【"+tblkey+"="+keyvalue+"】对应的的blob字段【"+blobfield+"】操作错误,原因:提供信息不全！");
        }
    	
    	DBConnection conn = null;
        PreparedStatement p_stmt = null; //
        ResultSet rs=null;

        long ll_1 = System.currentTimeMillis();
        try {
            if (ds == null) {
                ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
            }
            conn = new DBInitContext().getConn(ds); // 取得数据库连接!
            //判断是否存在记录
            String sql="select * from "+tblname+" where "+tblkey+"="+keyvalue;
            p_stmt = conn.prepareStatement(sql);
            rs=p_stmt.executeQuery();
            if(!rs.next()){            	
            	rs.close();
            	p_stmt.close();
            	//尝试插入记录，同时进行初始化
            	p_stmt = conn.prepareStatement("insert into "+tblname+"("+tblkey+","+blobfield+") values("+keyvalue+",EMPTY_BLOB()) ");
            	p_stmt.executeUpdate();
            	p_stmt.close();
            	p_stmt = conn.prepareStatement("select "+blobfield+" from "+tblname+" where "+tblkey+"="+keyvalue+" for update ");
                rs=p_stmt.executeQuery();
            	rs.next();
            }else{
            	rs.close();
            	p_stmt.close();
            	//执行初始化操作，无论是否有值统统清空初始化
            	p_stmt = conn.prepareStatement("update "+tblname+" set "+blobfield+"=EMPTY_BLOB() where "+tblkey+"="+keyvalue);
            	p_stmt.executeUpdate();
            	p_stmt.close();
            	p_stmt = conn.prepareStatement("select "+blobfield+" from "+tblname+" where "+tblkey+"="+keyvalue+" for update ");
                rs=p_stmt.executeQuery();
            	rs.next();
            }
        	oracle.sql.BLOB blob = (oracle.sql.BLOB) rs.getBlob(1);
        	if(blob != null)
        	{
				OutputStream out=blob.getBinaryOutputStream();
				out.write(data, 0, data.length);
				out.flush();
				out.close();
        	}
			
			long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
     			logger.debug("在数据源[" + ds + "]上执行SQL[" + sql + "]耗时[" + ll_dealtime + "]");
     		 }
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之["+sql+"]");
            }
			
            writeLog("对表【"+tblname+"】中主键【"+tblkey+"="+keyvalue+"】对应的的blob字段【"+blobfield+"】操作。");
        } catch (SQLException ex) {
        	logger.error("对表【"+tblname+"】中blob字段【"+blobfield+"】更新异常！", ex);
            Vector errInd = DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getErrDesc(ex, conn);
            if (errInd == null) {
                throw new SQLException("在数据源[" + ds + "]上执行对表【"+tblname+"】中主键【"+tblkey+"="+keyvalue+"】对应的的blob字段【"+blobfield+"】操作出错,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
            } else
                throw DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getDBErrExplainer().resolve(errInd);
        } catch (Exception ex) {
        	logger.error("对表【"+tblname+"】中blob字段【"+blobfield+"】更新异常！", ex);
            throw new Exception("在数据源[" + ds + "]执行对表【"+tblname+"】中主键【"+tblkey+"="+keyvalue+"】对应的的blob字段【"+blobfield+"】操作错误,原因:" + ex.getMessage());
        } finally {
        	try { if (rs != null) { rs.close(); } } catch (Exception ex) { }
            try { if (p_stmt != null) { p_stmt.close(); } } catch (Exception ex) { }
        }
    }//TODO 针对Clob字段处理大同小异，但对input/output Stream稍有不同
    
    /**
     * 读取clob字段（仅针对Oracle）
     * @param _datasourcename
     * @param _sql 只允许形如Select clobfield,.... from table where ...     
     * @return
     */
    public String readClobDataByDS(String ds, String sql)throws Exception{
    	if(sql==null||sql.trim().equals("")){
        	throw new Exception("在数据源[" + ds + "]执行SQL错误:" + sql + ",原因:提供信息不全！");
        }
    	
    	DBConnection conn = null;
        PreparedStatement p_stmt = null; //
        ResultSet rs=null;      
        String rt=null;
        
        long ll_1 = System.currentTimeMillis();
        try {
            if (ds == null) {
                ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
            }
            conn = new DBInitContext().getConn(ds); // 取得数据库连接!
            p_stmt = conn.prepareStatement(sql); // 创建游标,凡是一次远程调用都是在同一事务中进行,即取得的是同一个Connection!!
            rs=p_stmt.executeQuery();
			if(rs.next()){
				Clob clob = rs.getClob(1);
				if(clob != null)
					rt = clob.getSubString(1L, (int)clob.length());	
			}
			long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
     			logger.debug("在数据源[" + ds + "]上执行SQL[" + sql + "]耗时[" + ll_dealtime + "]");
     		 }
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之["+sql+"]");
            }
			
            writeLog(sql);   
            return rt;            
        } catch (SQLException ex) {
        	logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
            Vector errInd = DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getErrDesc(ex, conn);
            if (errInd == null) {
                throw new SQLException("在数据源[" + ds + "]上执行SQL[" + sql + "]出错,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
            } else
                throw DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getDBErrExplainer().resolve(errInd);
        } catch (Exception ex) {
        	logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
            throw new Exception("在数据源[" + ds + "]执行SQL错误:" + sql + ",原因:" + ex.getMessage());
        } finally {
        	try { if (rs != null) { rs.close(); } } catch (Exception ex) { }
            try { if (p_stmt != null) { p_stmt.close(); } } catch (Exception ex) { }
        }
    }
    
    /**
     * 读取Blob字段（仅针对Oracle）
     * @param _datasourcename
     * @param _sql 只允许形如Select blobfield,.... from table where ...     
     * @return
     */
    public byte[] readBlobDataByDS(String ds, String sql)throws Exception{
    	if(sql==null||sql.trim().equals("")){
        	throw new Exception("在数据源[" + ds + "]执行SQL错误:" + sql + ",原因:提供信息不全！");
        }
    	
    	DBConnection conn = null;
        PreparedStatement p_stmt = null; //
        ResultSet rs=null;
        ByteArrayOutputStream bao=new ByteArrayOutputStream();        
        byte[] rt=null;
        
        long ll_1 = System.currentTimeMillis();
        try {
            if (ds == null) {
                ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
            }
            conn = new DBInitContext().getConn(ds); // 取得数据库连接!
            p_stmt = conn.prepareStatement(sql); // 创建游标,凡是一次远程调用都是在同一事务中进行,即取得的是同一个Connection!!
            rs=p_stmt.executeQuery();
			if(rs.next()){
				BufferedInputStream dbin=new BufferedInputStream(rs.getBinaryStream(1));
				byte[] b=new byte[2048];	
				int len=dbin.read(b);
				while(len>0){					
					bao.write(b,0,len);
					len=dbin.read(b);
				}
				rt=bao.toByteArray();				
			}
			long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
     			logger.debug("在数据源[" + ds + "]上执行SQL[" + sql + "]耗时[" + ll_dealtime + "]");
     		 }
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之["+sql+"]");
            }
			
            writeLog(sql);   
            return rt;            
        } catch (SQLException ex) {
        	logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
            Vector errInd = DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getErrDesc(ex, conn);
            if (errInd == null) {
                throw new SQLException("在数据源[" + ds + "]上执行SQL[" + sql + "]出错,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
            } else
                throw DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getDBErrExplainer().resolve(errInd);
        } catch (Exception ex) {
        	logger.error("在数据源[" + ds + "]上执行SQL[" + sql + "]出错！", ex);
            throw new Exception("在数据源[" + ds + "]执行SQL错误:" + sql + ",原因:" + ex.getMessage());
        } finally {
        	try { if (rs != null) { rs.close(); } } catch (Exception ex) { }
            try { if (p_stmt != null) { p_stmt.close(); } } catch (Exception ex) { }
        }
    }//TODO 针对Clob字段处理大同小异，但对input/output Stream稍有不同
    
    /**
     * 读取Blob字段（仅针对Oracle）
     * @param _datasourcename
     * @param tblname
     * @param tblkey
     * @param keyvalue
     * @param blobfield
     * @return
     * @throws Exception
     */
    public byte[] readBlobDataByDS(String ds, String tblname, String tblkey, String keyvalue, String blobfield) throws Exception {
    	if(tblname==null||tblname.trim().equals("")
        		||tblkey==null||tblkey.trim().equals("")
        		||keyvalue==null||keyvalue.trim().equals("")
        		){
        	throw new Exception("在数据源[" + ds + "]执行对表【"+tblname+"】中主键【"+tblkey+"="+keyvalue+"】对应的的blob字段【"+blobfield+"】操作错误,原因:提供信息不全！");
        }
    	
    	DBConnection conn = null;
        PreparedStatement p_stmt = null; //
        ResultSet rs=null;
        ByteArrayOutputStream bao=new ByteArrayOutputStream();
        byte[] rt=null;

        long ll_1 = System.currentTimeMillis();
        try {
            if (ds == null) {
                ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
            }
            conn = new DBInitContext().getConn(ds); // 取得数据库连接!
            //判断是否存在记录
            String sql="select "+blobfield+" from "+tblname+" where "+tblkey+"="+keyvalue;
            p_stmt = conn.prepareStatement(sql);
            rs=p_stmt.executeQuery();
            if(!rs.next()){            	
            	return null;
            }
            BufferedInputStream dbin=new BufferedInputStream(rs.getBinaryStream(1));
			byte[] b=new byte[2048];	
			int len=dbin.read(b);
			while(len>0){					
				bao.write(b,0,len);
				len=dbin.read(b);
			}
			rt=bao.toByteArray();
			long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
     			logger.debug("在数据源[" + ds + "]上执行SQL[" + sql + "]耗时[" + ll_dealtime + "]");
     		 }
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之["+sql+"]");
            }
            writeLog("对表【"+tblname+"】中主键【"+tblkey+"="+keyvalue+"】对应的的blob字段【"+blobfield+"】操作。");
            return rt;
        } catch (SQLException ex) {
        	logger.error("对表【"+tblname+"】中blob字段【"+blobfield+"】更新异常！", ex);
            Vector errInd = DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getErrDesc(ex, conn);
            if (errInd == null) {
                throw new SQLException("在数据源[" + ds + "]上执行对表【"+tblname+"】中主键【"+tblkey+"="+keyvalue+"】对应的的blob字段【"+blobfield+"】操作出错,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
            } else
                throw DatabaseAdapter.getInstance(DBInfoConst.DATABASE_TYPE_ORACLE).getDBErrExplainer().resolve(errInd);
        } catch (Exception ex) {
        	logger.error("对表【"+tblname+"】中blob字段【"+blobfield+"】更新异常！", ex);
            throw new Exception("在数据源[" + ds + "]执行对表【"+tblname+"】中主键【"+tblkey+"="+keyvalue+"】对应的的blob字段【"+blobfield+"】操作错误,原因:" + ex.getMessage());
        } finally {
        	try { if (rs != null) { rs.close(); } } catch (Exception ex) { }
            try { if (p_stmt != null) { p_stmt.close(); } } catch (Exception ex) { }
        }
    }//TODO 针对Clob字段处理大同小异，但对input/output Stream稍有不同
    
    /**
     * 得到某一个序列的下一值!
     * @param _datasourcename
     * @param _sequenceName
     * @return
     * @throws Exception
     */
    public String getSequenceNextValByDS(String ds, String _sequenceName) throws Exception {
        if (ds == null) {
            ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }

        String[][] str_data = getStringArrayByDS(ds, "select " + _sequenceName + ".nextval from dual"); //
        return str_data[0][0];
    }

    /**
     * 调用存储过程不返回值 创建日期：(2005-1-13 11:25:26)
     *
     * @return java.lang.String[]
     * @param dnsName
     *            java.lang.String
     * @param procudereName
     *            java.lang.String
     * @param parmeter
     *            java.lang.String[]
     * @exception java.rmi.RemoteException
     *                异常说明。
     */
    public void callProcedureByDS(String ds, String procedureName, String[] parmeters) throws Exception {
        long ll_1 = System.currentTimeMillis();
        if (ds == null) {
            ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }
        DBConnection conn = null;
        java.sql.CallableStatement proc = null; //
        String str_pars = procedureName + "(";
        if (parmeters != null) {
            for (int i = 0; i < parmeters.length; i++) {
                if (parmeters[i] == null) {
                    str_pars = str_pars + "null,";
                } else {
                    str_pars = str_pars + "'" + parmeters[i] + "',";
                }
            }
            str_pars = str_pars.substring(0, str_pars.length() - 1);
        }
        str_pars = str_pars + ")";

        try {
            conn = getConn(ds); //
            String strTemp = "{ call " + procedureName + "(";
            if (parmeters != null) {
                for (int i = 0; i < parmeters.length; i++) {
                    strTemp = strTemp + "?,";
                }
                strTemp = strTemp.substring(0, strTemp.length() - 1);
            }
            strTemp = strTemp + ")}";

            proc = conn.prepareCall(strTemp);
            if (parmeters != null) {
                for (int i = 0; i < parmeters.length; i++) {
                    proc.setString(i + 1, parmeters[i]);
                }
            } // 设置入参
            proc.execute();

            long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
     			logger.debug("在数据源[" + ds + "]上执行SQL[" + str_pars + "]耗时[" + ll_dealtime + "]");
     		 }
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之["+str_pars+"]");
            }
        } catch (SQLException ex) {
        	logger.error("在数据源[" + ds + "]上执行存储过程[" + str_pars + "]出错！", ex);
            throw new SQLException("在数据源[" + ds + "]上调用存储过程[" + str_pars + "]失败,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
        } catch (Exception ex) {
        	logger.error("在数据源[" + ds + "]上执行存储过程[" + str_pars + "]出错！", ex);
            throw new Exception("在数据源[" + ds + "]上调用存储过程[" + str_pars + "]失败,失败原因:" + ex.getMessage());
        } finally {
            try { if (proc != null) proc.close(); } catch (Exception ex) { }            
        }
    }

    /**
     * 调用存储过程返回值 创建日期：(2005-1-13 11:25:26)
     *
     * @return java.lang.String[]
     * @param dnsName
     *            java.lang.String
     * @param procudereName
     *            java.lang.String
     * @param parmeter
     *            java.lang.String[]
     * @exception java.rmi.RemoteException
     *                异常说明。
     */
    public String callProcedureReturnStrByDS(String ds, String procedureName, String[] parmeters) throws
        Exception {
        DBConnection conn = null;
        java.sql.CallableStatement proc = null;

        long ll_1 = System.currentTimeMillis();
        String str_pars = procedureName + "(";
        if (parmeters != null) {
            for (int i = 0; i < parmeters.length; i++) {
                if (parmeters[i] == null) {
                    str_pars = str_pars + "null,";
                } else {
                    str_pars = str_pars + "'" + parmeters[i] + "',";
                }
            }
            str_pars = str_pars.substring(0, str_pars.length() - 1);
        }
        str_pars = str_pars + ")";

        try {
            if (ds == null) {
                ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
            }
            conn = getConn(ds); //
            String strTemp = "{ call " + procedureName + "(";
            if (parmeters != null) {
                for (int i = 0; i < parmeters.length; i++) {
                    strTemp = strTemp + "?,";
                }
                strTemp = strTemp.substring(0, strTemp.length() - 1);
            }
            strTemp = strTemp + ")}";

            proc = conn.prepareCall(strTemp);
            if (parmeters != null) {
                for (int i = 0; i < parmeters.length - 1; i++) {
                    proc.setString(i + 1, parmeters[i]);
                } // 设置入参
            }
            proc.registerOutParameter(parmeters.length, java.sql.Types.VARCHAR); // 设置出参
            proc.execute();
            String ls_return = String.valueOf(proc.getObject(parmeters.length)); // 得到出参
            
            long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
     			logger.debug("在数据源[" + ds + "]上执行SQL[" + str_pars + "]耗时[" + ll_dealtime + "]");
     		 }
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之["+str_pars+"]");
            }
            
            return ls_return; // 返回出参
        } catch (SQLException ex) { //
        	logger.error("在数据源[" + ds + "]上执行存储过程[" + str_pars + "]出错！", ex);
            throw new SQLException("在数据源[" + ds + "]上调用存储过程[" + str_pars + "]失败,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage()); //
        } catch (Exception ex) { //
        	logger.error("在数据源[" + ds + "]上执行存储过程[" + str_pars + "]出错！", ex);
            throw new Exception("在数据源[" + ds + "]上调用存储过程[" + str_pars + "]失败,失败原因:" + ex.getMessage()); //
        } finally {
            try { if (proc != null) proc.close(); } catch (Exception ex) { }
        }
    }

    /**
     * 调用函数返回字符串 创建日期：(2005-1-13 11:25:26)
     *
     * @return java.lang.String[]
     * @param dnsName
     *            java.lang.String
     * @param procudereName
     *            java.lang.String
     * @param parmeter
     *            java.lang.String[]
     * @exception java.rmi.RemoteException
     *                异常说明。
     */
    public String callFunctionReturnStrByDS(String ds, String functionName, String[] parmeters) throws
        Exception {
        if (ds == null) {
            ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }
        DBConnection conn = null;
        java.sql.CallableStatement proc = null;

        long ll_1 = System.currentTimeMillis();
        String str_pars = functionName + "(";
        if (parmeters != null) {
            for (int i = 0; i < parmeters.length; i++) {
                if (parmeters[i] == null) {
                    str_pars = str_pars + "null,";
                } else {
                    str_pars = str_pars + "'" + parmeters[i] + "',";
                }
            }
            str_pars = str_pars.substring(0, str_pars.length() - 1);
        }
        str_pars = str_pars + ")";
        
        try {
            conn = getConn(ds); //
            String strTemp = "{ ? = call " + functionName + "(";
            if (parmeters != null) { // 如果入参不为空,则拼成参数集
                for (int i = 0; i < parmeters.length; i++) {
                    strTemp = strTemp + "?,";
                }
                strTemp = strTemp.substring(0, strTemp.length() - 1);
            }
            strTemp = strTemp + ")}";

            proc = conn.prepareCall(strTemp);
            proc.registerOutParameter(1, java.sql.Types.VARCHAR); // 注册返回出参数,第一个总是游标类型
            if (parmeters != null) {
                for (int i = 0; i < parmeters.length; i++) {
                    proc.setString(i + 2, parmeters[i]);
                }
            }
            proc.execute();
            String str_return = String.valueOf(proc.getObject(1));
            
            long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
     			logger.debug("在数据源[" + ds + "]上执行SQL[" + str_pars + "]耗时[" + ll_dealtime + "]");
     		 }
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之["+str_pars+"]");
            }
            
            return str_return;
        } catch (SQLException ex) {
        	logger.error("在数据源[" + ds + "]上执行存储过程[" + str_pars + "]出错！", ex);
            throw new SQLException("在数据源[" + ds + "]上调用函数[" + str_pars + "]失败,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
        } catch (Exception ex) {
        	logger.error("在数据源[" + ds + "]上执行存储过程[" + str_pars + "]出错！", ex);
            throw new Exception("在数据源[" + ds + "]上调用函数[" + str_pars + "]失败,失败原因:" + ex.getMessage());
        } finally {
            try { if (proc != null) proc.close(); } catch (Exception ex) { }
        }
    }

    /**
     * 调用函数返回表 创建日期：(2005-1-13 11:25:26)
     *
     * @return java.lang.String[]
     * @param dnsName
     *            java.lang.String
     * @param procudereName
     *            java.lang.String
     * @param parmeter
     *            java.lang.String[]
     * @exception java.rmi.RemoteException
     *                异常说明。
     */
    public String[][] callFunctionReturnTableByDS(String ds, String functionName, String[] parmeters) throws
        Exception {
        if (ds == null) {
            ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }

        DBConnection conn = null; //
        java.sql.CallableStatement proc = null;
        java.sql.ResultSet rs = null;

        String str_pars = functionName + "(";
        if (parmeters != null) {
            for (int i = 0; i < parmeters.length; i++) {
                if (parmeters[i] == null) {
                    str_pars = str_pars + "null,";
                } else {
                    str_pars = str_pars + "'" + parmeters[i] + "',";
                }
            }
            str_pars = str_pars.substring(0, str_pars.length() - 1);
        }
        str_pars = str_pars + ")";

        
        try {
            conn = getConn(ds); //

            long ll_1 = System.currentTimeMillis();
            String strTemp = "{ ? = call " + functionName + "(";
            if (parmeters != null) { // 如果入参不为空,则拼成参数集
                for (int i = 0; i < parmeters.length; i++) {
                    strTemp += "?,";
                }
                strTemp = strTemp.substring(0, strTemp.length() - 1);
            }
            strTemp += ")}";

            proc = conn.prepareCall(strTemp);

            String[][] str_data = null; // 返回数据

            // 如果是ORACLE,则使用游标类型
            proc.registerOutParameter(1, oracle.jdbc.driver.OracleTypes.CURSOR); // 注册返回出参数,第一个总是游标类型
            if (parmeters != null) {
                for (int i = 0; i < parmeters.length; i++) {
                    proc.setString(i + 2, parmeters[i]);
                }
            }
            proc.execute();
            rs = (ResultSet) proc.getObject(1);
            if (rs != null) { // 如果结果集不为空
                Vector aVector = new Vector();
                int nColCnt = rs.getMetaData().getColumnCount();
                while (rs.next()) { // 遍历结果集
                    String[] str_rowdata = new String[nColCnt];
                    for (int i = 1; i <= nColCnt; i++) {
                        str_rowdata[i - 1] = rs.getString(i);
                    }
                    aVector.add(str_rowdata);
                }

                str_data = new String[aVector.size()][nColCnt];
                for (int i = 0; i < aVector.size(); i++) {
                    String[] str_rowdata = (String[]) aVector.get(i);
                    for (int j = 0; j < nColCnt; j++) {
                        str_data[i][j] = str_rowdata[j];
                    }
                }
            }
            long ll_2 = System.currentTimeMillis();
            long ll_dealtime = ll_2 - ll_1;
            if("true".equalsIgnoreCase((String)CoolServerEnvironment.getInstance().get("DS_PRINTSQL"))){
     			logger.debug("在数据源[" + ds + "]上执行SQL[" + str_pars + "]耗时[" + ll_dealtime + "]");
     		 }
            if (ll_dealtime > getWarnSQLTime()) {
                logger.warn("警告:在数据源[" + ds + "]上执行SQL耗时[" + ll_dealtime + "]超过[" + getWarnSQLTime() + "],可能要优化之["+str_pars+"]");
            }

            return str_data;
        } catch (SQLException ex) {
        	logger.error("在数据源[" + ds + "]上执行存储过程[" + str_pars + "]出错！", ex);
            throw new SQLException("在数据源[" + ds + "]上调用函数[" + str_pars + "]失败,错误编码[" + ex.getErrorCode() +
                                   "],状态[" + ex.getSQLState() + "],原因:" + ex.getMessage());
        } catch (Exception ex) {
        	logger.error("在数据源[" + ds + "]上执行存储过程[" + str_pars + "]出错！", ex);
            throw new Exception("在数据源[" + ds + "]上调用函数[" + str_pars + "]失败,失败原因:" + ex.getMessage());
        } finally {
            try {if (rs != null) rs.close(); } catch (Exception ex) { }
            try {if (proc != null) proc.close(); } catch (Exception ex) { }
        }
    }

    /**
     * 执行一次查询超过多少毫秒后就警告的上限!!!
     * @return
     */
    private long getWarnSQLTime() {
        return 3000;
    }

    
    
    private void writeLog(String sql) {
        //TODO 写日志
    }
    
    /**
     * 提交线程事务
     * @throws Exception
     * @deprecated 直接调用NovaInitContext类的 commit()、rollback()和release()
     */
    public int commitAll() throws Exception{ 
    	if (initContext == null) {
            initContext = new DBInitContext();
        }
    	return initContext.commit();
    }
    /**
     * 提交线程事务
     * @throws Exception
     */
    public int commit(String ds) throws Exception{
    	if (initContext == null) {
            initContext = new DBInitContext();
        }
    	if (ds == null||ds.trim().equals("")) {
    		ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }
    	return initContext.commit(ds);
    }
    /**
     * 回滚线程事务
     * @throws Exception
     * @deprecated 直接调用NovaInitContext类的 commit()、rollback()和release()
     */
    public int rollbackAll() throws Exception{
    	if (initContext == null) {
            initContext = new DBInitContext();
        }
    	return initContext.rollback();    	
    }
    /**
     * 回滚线程事务
     * @throws Exception
     */
    public int rollback(String ds) throws Exception{
    	if (initContext == null) {
            initContext = new DBInitContext();
        }
    	if (ds == null||ds.trim().equals("")) {
    		ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }
    	return initContext.rollback(ds);    	
    }

    /**
     * 清空线程缓冲信息
     * 也可以直接调用NovaInitContext类的 commit()、rollback()和release()
     */
    public void releaseContext() {
    	if (initContext == null) {
            initContext = new DBInitContext();
        }
    	try{
    		initContext.release();    	
    	}catch(Exception e){
    		System.out.println(e.getMessage());    		
    	}
    }
    /**
     * 清空线程缓冲信息
     * 直接调用NovaInitContext类的 commit()、rollback()和release()
     */
    public void releaseContext(String ds) {
    	if (initContext == null) {
            initContext = new DBInitContext();
        }
    	if (ds == null||ds.trim().equals("")) {
    		ds = CoolServerEnvironment.getInstance().getDefaultDataSourceName();
        }
    	try{
    		initContext.release(ds);    	
    	}catch(Exception e){
    		e.printStackTrace();
    	}	
    }
    /**
     * 关闭Connection
     * @deprecated 直接调用NovaInitContext类的 commit()、rollback()和release()
     */
    public void closeConn() {
    	;//不需要执行了	
    }
    /**
     * 关闭Connection
     * @deprecated 直接调用NovaInitContext类的 commit()、rollback()和release()
     */
    public void closeConn(String ds) {
    	;//不需要执行了	
    }
    
    /**
	 * 绑定参数
     * @throws SQLException 
	 */
	private void bindParameter(PreparedStatement psta, Object... objects) throws SQLException {
		int i = 1;
		for (Object ob : objects) {
			if (ob == null) {
				psta.setNull(i, Types.VARCHAR);
			}
			else if (ob instanceof Timestamp){
				psta.setTimestamp(i, (Timestamp) ob);
			}
			else if (ob instanceof java.util.Date) {
//				java.util.Date d = (java.util.Date) ob;
//				Date sqlDate = new Date(d.getTime());
//				psta.setDate(i, sqlDate);
				long time = ((java.util.Date) ob).getTime();
				Timestamp timestamp = new Timestamp(time);
				psta.setTimestamp(i, (Timestamp) timestamp);
			} 
			else {
				psta.setObject(i, ob);
			}
			i++;
		}
	}

	private String getPreparedSqlInfo(String sql, Object...parameters){
		StringBuffer result = new StringBuffer(sql);
		if(parameters.length == 0)
			return result.toString();
		result.append("   {");
		for (Object ob : parameters) {
			result.append(ob);
			result.append(",");
		}
		result.deleteCharAt(result.lastIndexOf(","));
		result.append("}");
		return result.toString();
	}
	
}
