package com.cool.sample;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.cool.common.constant.DMOConst;
import com.cool.common.logging.CoolLogger;
import com.cool.common.vo.HashVO;
import com.cool.dbaccess.CommDMO;

/**
 * @author zzy
 * @date Jul 23, 2012
 **/
public class DbAccessSample {

	private Logger logger = CoolLogger.getLogger(this.getClass());
	private CommDMO dmo = new CommDMO();

	public void jdbcTest() throws Exception {
		logger.info("SQLSERVER查询测试1...");
		String querySql = "select * from logintable";// 查询SQL,可能直接在PLSQL中执行

		// 执行查询语句，指定数据源、查询SQL, 返回HashVO对象数组
		HashVO[] vos = dmo.getHashVoArrayByDS("datasource_apmsold", querySql);
		for (int i = 0; i < vos.length; i++) {
			HashVO vo = vos[i];
			// 对查询到的数据进行处理，如打印出来
			String userid = vo.getStringValue("user_id");// 用位置索引 查询属性值
			String poste = vo.getStringValue("poste");// 用属性名查询属性值

			logger.info("查询到的数据[" + i + "]userid=" + userid + ", poste=" + poste);
		}
	}

	/**
	 * 从数据库中读取数据示例1
	 * 
	 * @throws Exception
	 */
	public void readData() throws Exception {

		logger.info("数据查询测试1...");
		String querySql = "select USERNAME,LOGINNAME from OPERATORUSER";// 查询SQL,可能直接在PLSQL中执行

		// 执行查询语句，指定数据源、查询SQL, 返回HashVO对象数组
		HashVO[] vos = dmo.getHashVoArrayByDS(null, querySql);
		for (int i = 0; i < vos.length; i++) {
			HashVO vo = vos[i];
			// 对查询到的数据进行处理，如打印出来
			String tableName = vo.getStringValue(0);// 用位置索引 查询属性值
			String comments = vo.getStringValue("LOGINNAME");// 用属性名查询属性值

			logger.info("查询到的数据[" + i + "]taleName=" + tableName + ", comments=" + comments);
		}
	}

	/**
	 * 从数据库中读取数据示例2
	 * 
	 * @throws Exception
	 */
	public void readDataUseParam() throws Exception {

		logger.info("数据查询测试2...");

		// 查询SQL,使用绑定变量，传入参数， "?"变绑定变量,可以有多个
		String querySql = "select sum(VALUE) as y0_, PERIOD as y1_,  BIZDATAID as y2_  from HISTORYDATA where " + "PERIOD>=? and PERIOD<=? and BIZDATAID in (" + "-1,1" + ") and "
				+ "( (productid = -1 and organizationid = -1)  or (productid = 242 and organizationid=75 )  )   group by PERIOD, BIZDATAID";

		// 执行查询语句，指定数据源、查询SQL,绑定变量参数
		HashVO[] vos = dmo.getHashVoArrayByDS(null, querySql, 201101, 201203);
		for (int i = 0; i < vos.length; i++) {
			HashVO vo = vos[i];
			// 对查询到的数据进行处理，如打印出来
			Long name = vo.getLongValue(0);// 用位置索引 查询属性值
			Integer y1_ = vo.getIntegerValue(1);
			Long y2_ = vo.getLognValue(2);

			logger.info("查询到的数据[" + i + "] y0_=" + name + ", y1_=" + y1_ + ", y2_=" + y2_);
		}

	}

	public void queryLogData(String datasource, String querySql) throws Exception {

		// 执行查询语句，指定数据源、查询SQL,绑定变量参数
		HashVO[] vos = dmo.getHashVoArrayByDS(null, querySql);
		for (int i = 0; i < vos.length; i++) {
			HashVO vo = vos[i];
			// 对查询到的数据进行处理，如打印出来
			String col1Value = vo.getStringValue(0);// 用位置索引 查询属性值
			logger.info("查询到的数据[" + i + "] 第一个参数：" + col1Value);
		}

	}

	/**
	 * 新增数据
	 * 
	 * @throws Exception
	 */
	public void insertData() throws Exception {
		logger.info("数据插入测试...");

		String insertSql = "insert into pub_log(id,time,source,operation) values(s_pub_log.nextval,sysdate,?,?)";
		String source = "testuser";
		String operation = "数据库测试";
		// 执行SQL,参数：数据源、操作SQL,和N(0或多个)变量参数
		dmo.executeUpdateByDS(null, insertSql, source, operation);

		dmo.executeUpdateByDS(null, insertSql, source, "第二条测试记录");
		// 数据更新后，执行提交
		dmo.commit(null);

		logger.info("数据插入成功");

	}
	
	public void insertClobData() throws Exception{
		logger.info("数据插入测试(带有CLOB字段类型)");
		
		String mtcode = "metadatatest2";
		
		String insertSQL = "insert into pub_metadata_templet(id,name,code,type,updatetime) values(s_pub_metadata_templet.nextval,?,?,0,sysdate)";
		dmo.executeUpdateByDS(DMOConst.DS_DEFAULT, insertSQL,mtcode,mtcode);
		dmo.commit(DMOConst.DS_DEFAULT); 
		
		logger.info("数据插入成功！");
	}
	
	/**
	 * 数据更新测试(带有CLOB字段类型)
	 * @throws Exception
	 */
	public void updateClobData() throws Exception{
		logger.info("数据更新测试(带有CLOB字段类型)");
		
		String code = "metadatatest2";
		String content = "<a><b>修改后内容</b><c>二节点</c></a>";
		String updateSQL = "update pub_metadata_templet set name=?,scope=?,type=?,content=? where code=?";
		dmo.executeUpdateByDS(DMOConst.DS_DEFAULT, updateSQL,code,code,1, content ,code);
		dmo.commit(DMOConst.DS_DEFAULT);
		
		logger.info("数据插入成功！");
	}

	/**
	 * 更新数据
	 * 
	 * @throws Exception
	 */
	public void updateData() throws Exception {
		logger.info("数据更新测试...");

		String updateSql1 = "update pub_log set operation='更新测试1' where source = 'testuser'";
		String updateSql2 = "update pub_log set operation='更新测试2' where source = ?";
		String source = "testuser";
		// 执行SQL,参数：数据源、操作SQL,和N(0或多个)变量参数
		dmo.executeUpdateByDS(null, updateSql1);

		dmo.executeUpdateByDS(null, updateSql2, source);
		// 数据更新后，执行提交
		dmo.commit(null);

		logger.info("数据更新成功");
	}

	/**
	 * 删除数据
	 * 
	 * @throws Exception
	 */
	public void deleteData() throws Exception {
		logger.info("数据删除测试...");

		String delSql1 = "delete pub_log where source = 'testuser'";
		String delSql2 = "delete pub_log where source = ?";
		String source = "testuser";

		// 以下两个操作结果相同，均是删除source为testuser的记录，一个不使用变量，一个使用绑定变量
		dmo.executeUpdateByDS(null, delSql1);
		dmo.executeUpdateByDS(null, delSql2, source);
		// 数据更新后，执行提交
		dmo.commit(null);

		logger.info("数据删除成功");
	}

}