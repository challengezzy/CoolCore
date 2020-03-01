package com.cool.form;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.cool.common.constant.DMOConst;
import com.cool.common.logging.CoolLogger;
import com.cool.common.util.DBUtil;
import com.cool.common.util.StringUtil;
import com.cool.common.vo.HashVO;
import com.cool.common.vo.MetadataTempletVO;
import com.cool.dbaccess.CommDMO;

/**
 * 元数据模板服务
 * 
 * @author jerry
 * @date Oct 10, 2013
 */
public class MetadataTempletService {

	private Logger logger = CoolLogger.getLogger(this);

	private static Map<String, MetadataTempletVO> metadataTempletCache = new ConcurrentHashMap<String, MetadataTempletVO>();

	private static MetadataTempletService metadataService;

	public static MetadataTempletService getInstance() {
		if (metadataService != null)
			return metadataService;
		else
			metadataService = new MetadataTempletService();

		return metadataService;
	}

	public List<String> getGlobalMetadataTempletCodeList() throws Exception {
		logger.debug("获取全局元数据模板编码列表:");
		CommDMO dmo = new CommDMO();
		try {
			List<String> result = new ArrayList<String>();
			String sql = "select code from pub_metadata_templet ";
			HashVO[] vos = dmo.getHashVoArrayByDS(null, sql);
			for (HashVO vo : vos) {
				result.add(vo.getStringValue(0));
			}
			return result;

		} catch (Exception e) {
			logger.error("获取元数据模板编码时异常：", e);
			throw e;
		} finally {
			dmo.releaseContext(null);
		}
	}

	/**
	 * 以"模板code@@versionCode"格式返回
	 * 
	 * @return
	 * @throws Exception
	 */
	public List<String> getGlobalMetadataTempletCodeAndVersionCodeList() throws Exception {
		List<String> result = new ArrayList<String>();
		List<String> codeList = getGlobalMetadataTempletCodeList();
		for (String code : codeList) {
			MetadataTempletVO templet = getMetadataTemplet(code);
			if (templet != null)
				result.add(code + "@@" + templet.getVersionCode());
		}
		return result;
	}

	/**
	 * 获取指定scope的元数据模板
	 * 
	 * @param scope
	 * @return
	 * @throws Exception
	 */
	public List<String> getUserMetadataTempletCodeList(String scope) throws Exception {
		logger.debug("获取模块为[" + scope + "]元数据模板编码列表");
		CommDMO dmo = new CommDMO();
		try {
			List<String> result = new ArrayList<String>();
			String sql = "select code from pub_metadata_templet where scope=?";
			HashVO[] vos = dmo.getHashVoArrayByDS(null, sql, scope);
			for (HashVO vo : vos) {
				result.add(vo.getStringValue(0));
			}
			return result;

		} catch (Exception e) {
			logger.error("获取模块为[" + scope + "]元数据模板编码列表异常！", e);
			throw e;
		} finally {
			dmo.releaseContext(null);
		}

	}

	/**
	 * 直接从数据库中查询元数据
	 * 
	 * @param code
	 * @return
	 * @throws Exception
	 */
	public MetadataTempletVO getMetadataTempletNoCache(String code) throws Exception {
		clearCacheByMtCode(code);
		return getMetadataTemplet(code);
	}

	/**
	 * 根据code列表批量获取数据
	 * 
	 * @param codes
	 * @return
	 * @throws Exception
	 */
	public MetadataTempletVO[] getMetadataTempletByCodes(String[] codes) throws Exception {
		MetadataTempletVO[] metadataTemplets = null;
		if (codes != null) {
			metadataTemplets = new MetadataTempletVO[codes.length];
			for (int i = 0; i < codes.length; i++) {
				metadataTemplets[i] = getMetadataTemplet(codes[i]);
			}
		}
		return metadataTemplets;
	}

	public void updateMetadataTempletContent(String code, String content) throws Exception {
		logger.debug("更新元数据模板的内容[code=" + code + "]");
		CommDMO dmo = new CommDMO();
		try {
			dmo.executeUpdateClobByDS(null, "CONTENT", "pub_metadata_templet", "code='" + code + "'", content);
			dmo.commit(null);

		} catch (Exception e) {
			dmo.rollback(null);
			logger.error("更新元数据模板的内容[code=" + code + "]异常！", e);
			throw e;
		} finally {
			dmo.releaseContext(null);
		}
	}

	/**
	 * 清空元数据缓存
	 */
	public void clearCache() {
		logger.debug("清空元数据模板缓存");
		metadataTempletCache.clear();
	}

	/**
	 * 更新指定MT的缓存
	 * 
	 * @param code
	 * @return
	 */
	public String clearCacheByMtCode(String code) {
		logger.debug("更新元数据模板编码[" + code + "]刷新服务端缓存");
		if (metadataTempletCache.containsKey(code)) {
			metadataTempletCache.remove(code);
		}
		return code;
	}
	
	/**
	 * 根据元数据编码，获取元数据。先从缓存查找，缓存中未查到则从数据库中获取
	 * @param code
	 * @return
	 * @throws Exception
	 */
	public MetadataTempletVO getMetadataTemplet(String code) throws Exception {
		logger.debug("获取元数据模板[code=" + code + "]");
		if (metadataTempletCache.containsKey(code)) {
			
			return metadataTempletCache.get(code);
			
		} else {
			logger.debug("从数据库中读取元数据模板[code=" + code + "]");
			CommDMO dmo = new CommDMO();
			try{
				String sql = "select t.id,t.name,t.code,t.content,t.type,t.scope,t.modifier,t.updatetime from pub_metadata_templet t where code=?";
				HashVO[] vos = dmo.getHashVoArrayByDS(null, sql, code);
				if (vos.length == 0)
					throw new Exception("未找到code=["+code+"]的元数据模板!");
				
				HashVO vo = vos[0];
				
				MetadataTempletVO templet = new MetadataTempletVO();
				templet.setId(vo.getLongValue("id"));
				templet.setCode(vo.getStringValue("code"));
				templet.setName(vo.getStringValue("name"));
				templet.setContent(vo.getStringValue("content"));
				templet.setScope(vo.getStringValue("scope"));
				templet.setType(vo.getIntegerValue("type"));
				templet.setModifier(vo.getStringValue("modifier"));
				templet.setUpdateTime(vo.getDateValue("updatetime"));
				// templet.setVersionCode(UUID.randomUUID().toString());
				// 使用元模板定义的数据内容生成的hash值来确定是否客户端重新下载，这样可以避免服务端的重起导致的频繁下载
				templet.setVersionCode((vo.getStringValue("content") == null ? "" : vo.getStringValue("content")).hashCode() + "");
				
				metadataTempletCache.put(code, templet);
				
				return templet;
			} catch (Exception e) {
				logger.error("获取元数据模板[code=" + code + "]异常！", e);
				throw e;
			} finally {
				dmo.releaseContext(null);
			}
		}
	}

	
	public void saveOrUpdateMetadataTemplet(MetadataTempletVO metaVo) throws Exception {
		CommDMO dmo = new CommDMO();
		String code = metaVo.getCode();
		try {
			if (!StringUtil.isEmpty(code)) {
				HashVO[] vos = dmo.getHashVoArrayByDS(DMOConst.DS_DEFAULT, "select count(1) cou from pub_metadata_templet where code=?", code);
				if (vos != null && vos.length > 0) {
					if (vos[0].getIntegerValue("cou") > 0) {
						logger.debug("更新元数据模板的内容[code=" + code + "]");
						String updateSQL = "update pub_metadata_templet set content=?,name=?,scope=?,type=?,modifier=?,updatetime="+DBUtil.getSysDate()+" where code=?";
						dmo.executeUpdateByDS(DMOConst.DS_DEFAULT, updateSQL,metaVo.getContent(),metaVo.getName(),metaVo.getScope(),metaVo.getType(),metaVo.getModifier(), code);
						dmo.commit(DMOConst.DS_DEFAULT);
						
					} else {
						logger.debug("新增元数据模板的内容[code=" + code + "]");
						//CLOB字段直接插入没有问题
						String insertSQL = "insert into pub_metadata_templet("+DBUtil.getInsertId()+" name,code,content,modifier,scope,type,updatetime) " +
											"values("+ DBUtil.getSeqValue("s_pub_metadata_templet") +" ?,?,?,?,?,?,"+DBUtil.getSysDate()+")";
						dmo.executeUpdateByDS(DMOConst.DS_DEFAULT, insertSQL, metaVo.getName(), code,metaVo.getContent(), metaVo.getModifier(), metaVo.getScope(), metaVo.getType());
						dmo.commit(DMOConst.DS_DEFAULT);
					}
					
					clearCacheByMtCode(code);
				}
			}
		} catch (Exception e) {
			dmo.rollback(null);
			logger.error("元数据模板的内容[code=" + code + "]更新失败！", e);
			throw e;
		} finally {
			dmo.releaseContext(null);
		}
	}
	
	/**
	 * 删除元数据模板
	 * @param mtcode
	 * @throws Exception
	 */
	public void deleteMetadataTempletVo(String mtcode) throws Exception {
		logger.debug("删除元数据模板" + mtcode + "]");
		if (mtcode == null || "".equals(mtcode))
			throw new Exception("元数据模板编码不能为空!");
		CommDMO dmo = new CommDMO();
		String sql = "delete from pub_metadata_templet where code=?";
		try {
			dmo.executeUpdateByDS(null, sql, mtcode);
			dmo.commit(null);
			
			clearCacheByMtCode(mtcode);
		} catch (Exception e) {
			logger.error(e);
			throw e;
		}
	}

}
