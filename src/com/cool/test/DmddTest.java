package com.cool.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import com.cool.common.logging.CoolLogger;
import com.cool.common.system.CoolServerEnvironment;
import com.cool.dbaccess.DataSourceManager;
import com.cool.sample.DbAccessSample;


public class DmddTest {
	
	private static Logger logger= CoolLogger.getLogger(DmddTest.class);
	public static String configFileUrl = "com/cool/test/CoolTestConfig.xml";
	
	public static void main(String[] args) throws Exception{
		initDataSources();
//		
//		//数据库操作
		DbAccessSample db = new DbAccessSample();
//		db.readData();
//		db.readDataUseParam();
//		db.insertClobData();
		//db.updateClobData(); 
		
		int a=0;
		int c=0;
		do {
			--c;
			a = a - 1;
		} while (a > 0);

		System.out.println("a="+a+ ", c="+c);
//		
//		db.insertData();
//		db.queryLogData(ApmsConst.DS_DEFAULT,"select operation,id,time,source from pub_log where source = 'testuser'");
//		db.updateData();
//		db.queryLogData(ApmsConst.DS_DEFAULT,"select operation,id,time,source from pub_log where source = 'testuser'");
//		db.deleteData();
//		db.queryLogData(ApmsConst.DS_DEFAULT,"select operation,id,time,source from pub_log where source = 'testuser'");
	}
	
	private static void initDataSources() throws JDOMException, IOException{
		long ll_1 = System.currentTimeMillis();
		logger.debug("开始初始化数据库连接池...");
		
        InputStream is = DmddTest.class.getClassLoader().getResourceAsStream(configFileUrl);
        Document doc = new SAXBuilder().build(is);
        Element datasources = doc.getRootElement().getChild("datasources"); // 得到datasources子结点!!
        if (datasources != null) {
            List sources = datasources.getChildren("datasource"); // 得到所有子结点!!
            DataSourceManager.initDS(sources);
            
            //服务器端设置变量
            CoolServerEnvironment.getInstance().put("defaultdatasource", DataSourceManager.getDefaultDS()); // 设置默认数据源!!
            String[] keys=DataSourceManager.getDataSources();
            CoolServerEnvironment.getInstance().put("ALLDATASOURCENAMES",keys );
            for(int i=0;i<keys.length;i++){
                CoolServerEnvironment.getInstance().put(keys[i],DataSourceManager.getDataSourceUrl(keys[i]) ); //
            }
        }
        long ll_2 = System.currentTimeMillis();
        
        logger.debug("初始化数据库连接池结束,耗时[" + (ll_2 - ll_1) + "]");
	}

}
