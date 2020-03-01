package com.cool.dbaccess.test;



import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import com.cool.common.logging.CoolLogger;
import com.cool.dbaccess.DBConnection;
import com.cool.dbaccess.DBInitContext;


public class DataSourceMonitorServlet extends HttpServlet {

	/**
	 * Constructor of the object.
	 */
	public DataSourceMonitorServlet() {
		super();
	}

	/**
	 * Destruction of the servlet. <br>
	 */
	public void destroy() {
		super.destroy(); // Just puts "destroy" string in log
		// Put your code here
	}

	/**
	 * Initialization of the servlet. <br>
	 *
	 * @throws ServletException if an error occurs
	 */
	public void init() throws ServletException {
		//创建一个线程，循环执行（间隔30000ms），由某一共享变量控制停止。
		//  循环创建子线程，每个子线程执行下列步骤
		//    1、获得数据源列表
		//    2、执行连接探测（目前只支持Oracle）
		
		
		
//		System.err.println("断网实验开始！！！！");
//		//模拟远程事务调用
//		NovaInitContext context = new NovaInitContext(); //
//		try{
//			
//			
//			CommDMO dmo=new CommDMO(false);
//			
//			CoolLogger.getLogger(this).debug("现在开始可以随时断开物理网络！");
//			REDO:
//			for(int i=0;i<30;i++){
//				CoolLogger.getLogger(this).debug("执行一次SQL！序号："+i);
//				try{
//					if(i==0){dmo.executeUpdateByDS(null, "delete from pub_news");}
//					dmo.executeUpdateByDS(null, "insert into pub_news(pk_pub_news,title,content,isscrollview,createtime,creater)"
//							+" values("+i+",'测试网络自恢复"+i+"','测试网络自恢复"+i+"，是否成功呢？','N',to_date('2007-02-19','yyyy-MM-dd'),'James.W')");
//				}catch(Exception xx){
//					CoolLogger.getLogger(this).error(xx.getMessage());
//					
//					this.rollbackTrans(context);
//					this.closeConn(context);
//					this.releaseContext(context);
//										
//					i=-1;//重新执行的时候会先执行++，所以设为-1这样其实是从0开始。
//					CoolLogger.getLogger(this).debug("现在开始重新执行！");
//					continue REDO;
//				}
//				
//				delay(1000);
//				
//			}
//			this.commitTrans(context);
//		}catch(Exception e){
//			
//		}		
		
	}
	
	
	private void delay(long d){
		try{
			Thread.currentThread().sleep(d);		    
		}catch(InterruptedException x){
			CoolLogger.getLogger(this).error("不能执行延迟动作！");
		}
	}
	
	/**
	 * 提交所有事务
	 * @param _initContext
	 */
	private int commitTrans(DBInitContext _initContext) {
		//System.out.println("提交该次远程访问所有事务!"); //
		int li_allStmtCount = 0;
		if (_initContext.isGetConn()) {
			DBConnection[] conns = _initContext.GetAllConns();
			for (int i = 0; i < conns.length; i++) {
				try {
					li_allStmtCount = li_allStmtCount + conns[i].getOpenStmtCount(); //
					conns[i].transCommit();
				} catch (Throwable e) {
					CoolLogger.getLogger(this).error(e.getMessage());
				}
			}
		}
		return li_allStmtCount;
	}

	private int rollbackTrans(DBInitContext _initContext) {
		//System.out.println("回滚该次远程访问所有事务!"); //
		int li_allStmtCount = 0;
		if (_initContext.isGetConn()) {
			DBConnection[] conns = _initContext.GetAllConns();
			for (int i = 0; i < conns.length; i++) {
				try {
					li_allStmtCount = li_allStmtCount + conns[i].getOpenStmtCount(); //
					conns[i].transRollback();
				} catch (Throwable e) {
					CoolLogger.getLogger(this).error(e.getMessage());
				}
			}
		}
		return li_allStmtCount;
	}

	/**
	 * 关闭数据库连接
	 * @param _initContext
	 */
	private void closeConn(DBInitContext _initContext) {
		//System.out.println("关闭该次远程访问所有事务!"); //
		if (_initContext.isGetConn()) {
			DBConnection[] conns = _initContext.GetAllConns();
			for (int i = 0; i < conns.length; i++) {
				try {
					conns[i].close(); //关闭指定数据源连接
					//System.out.println("关闭当前远程访问用到的数据库连接[" + conns[i].getDsName() + "]");
				} catch (Throwable e) {
					CoolLogger.getLogger(this).error(e.getMessage());
				}
			}
		}
	}

	private void releaseContext(DBInitContext _initContext) {
		//System.out.println("释放该次远程访问所有资源!"); //
		try {
			_initContext.release(); //释放所有资源!!
		} catch (Throwable ex) {
			CoolLogger.getLogger(this).error(ex.getMessage());
		}

	}


}
