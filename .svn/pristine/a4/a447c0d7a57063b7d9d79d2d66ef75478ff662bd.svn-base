package com.cool.form;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cool.common.constant.DMOConst;
import com.cool.common.constant.SysConst;
import com.cool.common.logging.CoolLogger;
import com.cool.common.util.ExportInitMetaData;
import com.cool.common.util.XMLHandleUtil;
import com.cool.common.vo.HashVO;
import com.cool.common.vo.SimpleHashVO;
import com.cool.common.vo.TableDataStruct;
import com.cool.common.vo.XMLExportObject;
import com.cool.dbaccess.CommDMO;
import com.cool.dbaccess.DataSourceManager;
import com.cool.file.CsvExportService;
import com.cool.file.FileServletURI;

/**
 * 基础的Felx远程访问服务
 * @author jerry
 * @date Sep 18, 2013
 */
public class CoolFormService {
	
	protected Logger logger = CoolLogger.getLogger(this.getClass());

	public static final String DATASOURCE_CHAT = "datasource_chat";
	
	public static final String KEYNAME_MODIFYFLAG = "FORMSERVICE_MODIFYFLAG";
	
	public static final String KEYNAME_TEMPLETCODE = "FORMSERVICE_TEMPLETCODE";
	
	private CsvExportService fileService = new CsvExportService();
	
	
	public TableDataStruct getTableDataStructByDS(String ds,String sql) throws Exception {
		CommDMO dmo = new CommDMO();
		try {
			return dmo.getTableDataStructByDS(ds, sql);
		} catch (Exception e) {
			logger.error("getTableDataStructByDS 错误！", e);
			throw e;
		}finally {
			dmo.releaseContext();
		}
	}
	
	public SimpleHashVO[] getSimpleHashVoArrayByDS(String ds, String sql)
			throws Exception {
		CommDMO dmo = new CommDMO();
		try {
			HashVO[] vos = dmo.getHashVoArrayByDS(ds, sql);
			SimpleHashVO[] result = new SimpleHashVO[vos.length];
			for (int i = 0; i < vos.length; i++) {
				SimpleHashVO vo = new SimpleHashVO(vos[i]);
				result[i] = vo;
			}
			return result;
		} catch (Exception e) {
			logger.error("getSimpleHashVoArrayByDS 错误！", e);
			throw e;
		}finally {
			dmo.releaseContext(ds);
		}
	}
	
	public SimpleHashVO[] getSimpleHashVoArrayUnlimitedByDS(String ds, String sql) throws Exception {
		CommDMO dmo = new CommDMO();
		try {
			HashVO[] vos = dmo.getHashVoArrayByDSUnlimitRows(ds, sql);
			SimpleHashVO[] result = new SimpleHashVO[vos.length];
			for (int i = 0; i < vos.length; i++) {
				SimpleHashVO vo = new SimpleHashVO(vos[i]);
				result[i] = vo;
			}
			return result;
		} catch (Exception e) {
			logger.error("getSimpleHashVoArrayByDS 错误！", e);
			throw e;
		} finally {
			dmo.releaseContext(ds);
		}
	}

	/**
	 * 执行一个SQL语句，返回影响的行数
	 * @param ds
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	public int executeUpdateByDS(String ds, String sql) throws Exception {
		CommDMO dmo = new CommDMO();
		try {
			int r = dmo.executeUpdateByDS(ds, sql);
			dmo.commit(ds);
			return r;
		} catch (Exception e) {
			dmo.rollback(ds);
			logger.error("executeUpdateByDS 错误！", e);
			throw e;
		} finally {
			dmo.releaseContext(ds);
		}
	}

	/**
	 * 批量执行SQL语句
	 * @param ds
	 * @param sqls
	 * @return
	 * @throws Exception
	 */
	public int[] executeBatchByDS(String ds, String[] sqls) throws Exception {
		CommDMO dmo = new CommDMO();
		try {
			int[] r = dmo.executeBatchByDS(ds, sqls);
			dmo.commit(ds);
			return r;
		} catch (Exception e) {
			dmo.rollback(ds);
			logger.error("执行批量SQL数据库更新错误！", e);
			throw e;
		} finally {
			dmo.releaseContext(ds);
		}
	}


	/**
	 * 根据sql查询结果生成csv文件
	 * @param datasource
	 * @param sql
	 * @param dirPath
	 * @param columnMap 列名称映射
	 * @param charSet 字符编码集 如GBK,UTF-8
	 * @return
	 * @throws Exception
	 */
	public Map<String, Object> generateCSVFile(String datasource,String sql, String dirPath,Map<String, String> columnMap,String charSet) throws Exception{
		try{
		   return fileService.generateOlapCsvFile(datasource, sql, dirPath,columnMap,charSet);
		}catch (Exception e) {
			logger.error("根据SQL["+ sql +"]生成CSV文件异常！", e);
			throw e;
		}
		
	}
	
	/**
	 * 根据sql查询结果生成csv文件
	 * @param datasource
	 * @param sql
	 * @param dirPath
	 * @param columnMap 列名称映射
	 * @return 可下载的文件路径
	 * @throws Exception
	 */
	public String generateExportCSVFile(String datasource,String sql, String dirPath,Map<String, String> columnMap,String reportName) throws Exception{
		try{
		    String fileName =  fileService.generateCSVFile(datasource, sql, dirPath,columnMap,"gbk");
		    
		    FileServletURI fsu = new FileServletURI();
			return fsu.getDownLoadURI(fileName,reportName);
		}catch (Exception e) {
			logger.error("根据SQL["+ sql +"]生成CSV文件异常！", e);
			throw e;
		}
		
	}
	
	/**
	 * 查询分页数据
	 * 返回的MAP对象，包括记录总数和当前页数据的SimpleHashVO数组
	 */
	public Map<String, Object> getSimpleHashVOMapByPage(String dataSource,String sql,int pageNum, int rowCountPerPage)	throws Exception {
		Map<String, Object> result = getSimpleHashVOMapByPage(dataSource, sql, sql, pageNum, rowCountPerPage);
		
		return result;
	}
	
	/**
	 * 查询分页数据,查询总记录数和数据不是同一SQL
	 * 查询总记录数的SQL效率要高
	 * 返回的MAP对象，包括记录总数和当前页数据的SimpleHashVO数组
	 */
	public Map<String, Object> getSimpleHashVOMapByPage(String dataSource,String countSql,String dataSql,int pageNum, int rowCountPerPage)	throws Exception {
		CommDMO dmo = new CommDMO();
		Map<String, Object> result = new HashMap<String, Object>();
		try {
			int rowsCount = dmo.getRowsCountByDS(dataSource,countSql);
			Map<String, Object> hashvoMap = dmo.getHashVoArrayByPage(dataSource, dataSql, pageNum, rowCountPerPage,rowsCount);
			HashVO[] vos = (HashVO[])hashvoMap.get(DMOConst.HASHVOARRAY);
			
			SimpleHashVO[] simpleVos = new SimpleHashVO[vos.length];
			for (int i = 0; i < vos.length; i++) {
				SimpleHashVO vo = new SimpleHashVO(vos[i]);
				simpleVos[i] = vo;
			}
			
			result.put(DMOConst.ROWCOUNT, hashvoMap.get(DMOConst.ROWCOUNT));
			result.put(DMOConst.SIMPLEHASHVOARRAY, simpleVos);
			
			return result;
		}catch (Exception e) {
			logger.error("getSimpleHashVOArrayByPage 错误！", e);
			throw e;
		}finally {
			dmo.releaseContext();
		}
	}
	
	/**
	 * 查询所有数据，无数据量限制
	 * 返回的MAP对象，包括记录总数和当前页数据的SimpleHashVO数组
	 */
	public Map<String, Object> getSimpleHashVOMap(String dataSource,String sql)	throws Exception {
		CommDMO dmo = new CommDMO();
		Map<String, Object> result = new HashMap<String, Object>();
		try {
			HashVO[] vos = dmo.getHashVoArrayByDSUnlimitRows(dataSource, sql);
			
			SimpleHashVO[] simpleVos = new SimpleHashVO[vos.length];
			for (int i = 0; i < vos.length; i++) {
				SimpleHashVO vo = new SimpleHashVO(vos[i]);
				simpleVos[i] = vo;
			}
			result.put(DMOConst.SIMPLEHASHVOARRAY, simpleVos);
			
			return result;
		}catch (Exception e) {
			logger.error("getSimpleHashVOArrayByPage 错误！", e);
			throw e;
		}finally {
			dmo.releaseContext();
		}
	}
	
	/**
	 * 查询所有JDBC关系型数据源名称
	 * @return
	 * @throws Exception
	 */
	public String[] queryAllRelationDsName() throws Exception{
		try {
			return DataSourceManager.getDataSources();
		} catch (Exception e) {
			logger.error("查询所有JDBC关系型数据源名称错误!", e);
			throw e;
		}
	}
	
	/**
	 * 查询所有JDBC关系型数据源配置信息
	 * @return
	 * @throws Exception
	 */
	public HashMap queryAllRelationDsConfig() throws Exception{
		try {
			return DataSourceManager.getDataSourcesSet();
		} catch (Exception e) {
			logger.error("查询所有JDBC关系型数据源配置信息错误!", e);
			throw e;
		}
	}
	
	public String generateViewFile(String datasource,String sql,String reportName)
			throws Exception {
		try {
			String fileName = fileService.generateViewFile(datasource, sql,"download/","UTF-8");
			FileServletURI fsu = new FileServletURI();
			return fsu.getDownLoadURI(fileName, reportName);
		} catch (Exception e) {
			logger.error("生成SQL文件异常!", e);
			throw e;
		}
	}
	
	public String generateFile(String content,String reportName,String fileType) throws Exception {
		try{
			String fileName = fileService.generateFile(content,fileType,"download/","UTF-8");
			FileServletURI fsu = new FileServletURI();
			return fsu.getDownLoadURI(fileName,reportName);
		}catch(Exception e){
			logger.error("生成文件异常!", e);
			throw e;
		}
	}
	
	public void importXMLFromMDFile(byte[] content, String datasource,String flagName) throws Exception {

		CoolLogger.getLogger(this).debug("从MD文件导入数据");
		String UpLoadFileName = XMLHandleUtil.createUpLoadFile(content);
		String unZipDir = XMLHandleUtil.unZipFile(UpLoadFileName);
		if (!XMLHandleUtil.isValidFile(unZipDir,flagName)) {
			File file = new File(unZipDir);
			XMLHandleUtil.deleteFielOrDir(file);
			throw new Exception("无法导入,请选择正确的导出文件!");
		}
		XMLHandleUtil.importXMLFromMDFile(unZipDir, datasource);

	}
	
	public String exportXmlToMDFile(XMLExportObject[] exportObjects,String reportName,String flagName) throws Exception{
		CoolLogger.getLogger(this).debug("从数据库导出MD文件");
		FileServletURI fsu = new FileServletURI();
		String downLoadFile =  new ExportInitMetaData().exportXmlToMDFile(exportObjects,flagName);
		return fsu.getDownLoadURI(SysConst.DOWNLOAD_DIR +"/" + downLoadFile,reportName);
	}
	
	
}


