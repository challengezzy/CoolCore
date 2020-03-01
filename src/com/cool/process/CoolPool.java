/**********************************************************************
 *$RCSfile: WlanPool.java,v $  $Revision: 1.2 $  $Date: 2010/01/21 08:55:30 $
 *********************************************************************/ 
package com.cool.process;


import org.apache.log4j.Logger;

import com.cool.common.logging.CoolLogger;

/**
 * <li>Title: when the number of threads is greater than the core, this is the maximum time that excess idle threads will wait for new tasks before terminating.
.java</li>
 * <li>Description: 简介:用来缓存必要的Cool公共处理信息以及启动相关的单例机制</li>
 * <li>Project: wlan</li>
 * <li>Copyright: Copyright (c) 2009</li>
 * @Company: GXLU. All Rights Reserved.
 * @author luowang Of VAS2.Dept
 * @version 1.0
 */
public class CoolPool
{
	private final Logger logger = CoolLogger.getLogger(this.getClass());
	private static CoolPool theinstance = null;
	private ThreadProcessMgmt processMgmt=new ThreadProcessMgmt();
	/**
	 * 实例化-单例类
	 * @return
	 */
	public synchronized static CoolPool getinstance() 
	{
		if (theinstance == null) 
		{
			theinstance = new CoolPool();
		}
		return theinstance;
	}
	
	public void start()
	{
		startThreadProcess();
	}
	
	private CoolPool()
	{
		logger.info("实例化DmddPool...");
	}
	
	public void startThreadProcess()
	{
		processMgmt.start();
	}
	
	public void stopThreadProcess()
	{
		processMgmt.stopRequest();
	}
	
	public void addThreadProcessMgmt(IThreadProcess process) throws InterruptedException
	{
		processMgmt.add(process);
	}
	
	public void initServiceEnvironment()
	{
		
	}
}

/**********************************************************************
 *$RCSfile: WlanPool.java,v $  $Revision: 1.2 $  $Date: 2010/01/21 08:55:30 $
 *
 *$Log: WlanPool.java,v $
 *Revision 1.2  2010/01/21 08:55:30  luowang
 **** empty log message ***
 *
 *Revision 1.1  2009/12/15 01:28:29  luowang
 *�Զ�����
 *
 *
 *********************************************************************/