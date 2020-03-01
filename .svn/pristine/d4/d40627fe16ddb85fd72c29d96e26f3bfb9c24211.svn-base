package com.cool.process;

import java.util.List;

import org.apache.log4j.Logger;

import com.cool.common.logging.CoolLogger;

public abstract class AbstractTHreadMgmt 
{
	protected Logger logger = CoolLogger.getLogger(this.getClass());
	protected  int count=0;//计数器
	protected  int total=0;//处理process总数
	protected  volatile boolean  isok = false;//用来标记整个处理过程是否完成
	/**
	 * 执行方法
	 */
	public abstract void excute()  throws Exception;
	/**
	 * 用来处理回调结果，objects为从单个process中得到的处理结果
	 * @param objects
	 */
	public abstract  void doResult(Object...objects);
	
	/**
	 * 用来获取处理结果
	 * @return
	 */
	public abstract List<Object> getResult();
	
	//用来监控是否已经完成所有任务,直到完成才会结束线程
	protected void check()
	{
		while(true)
		{
			try
			{
				if(isOk())
				{
					Thread.interrupted();
					return ;
				}
				else 
				{					
					Thread.sleep( 1000 );					
				}
			}
			catch( InterruptedException e )
			{
				logger.info( "处理出现异常：",e );
			}
		}
	}
	protected void resume()
	{
		isok=true;
	}
	
	protected boolean isOk()
	{
		return isok;
	}
}
