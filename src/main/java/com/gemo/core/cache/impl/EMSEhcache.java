package com.gemo.core.cache.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.ehcache.EhCacheCache;
import org.springframework.cache.ehcache.EhCacheCacheManager;
import org.springframework.stereotype.Component;

import com.gemo.core.cache.EMSBaseCache;
import com.gemo.core.cache.EMSCache;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;

@Component("EMSEhcache")
@SuppressWarnings("unchecked")
public class EMSEhcache extends EMSBaseCache implements EMSCache {

	private static final Logger log = Logger.getLogger(EMSEhcache.class);
	
	@Resource(name="cacheManager")
	private EhCacheCacheManager cacheManager;
	
	@Value("${system.cacheInMemory}")
	private String cacheInMemory;
	
	@Override
	protected void putValue(String cacheName, String key, Object value) throws Exception {
		// TODO Auto-generated method stub
		EhCacheCache cache = (EhCacheCache)cacheManager.getCache(cacheName);
		if(cache==null){
			this.createCache(cacheName);
			cache = (EhCacheCache)cacheManager.getCache(cacheName);
		}
		Element element = new Element(key, value);
		cache.getNativeCache().put(element);
	}

	@Override
	protected <T> T getValue(String cacheName, String key) throws Exception {
		// TODO Auto-generated method stub
		EhCacheCache cache = (EhCacheCache)cacheManager.getCache(cacheName);
		if(cache==null){
			return null;
		}
		Element element = cache.getNativeCache().get(key);
		if(element!=null){
			return (T)element.getValue();
		}
		return null;
	}

	@Override
	public synchronized void createCache(String cacheName) {
		// TODO Auto-generated method stub
		if(cacheManager.getCache(cacheName)==null){
			Ehcache nativeCache = new Cache(cacheName,this.getCacheInMemory(),true,true,0,0);
			log.info("创建缓存：" + cacheName);
			cacheManager.getCacheManager().addCache(nativeCache);
		}
	}

	@Override
	public List<String> getKeyList(String cacheName) throws Exception {
		// TODO Auto-generated method stub
		EhCacheCache cache = (EhCacheCache)cacheManager.getCache(cacheName);
		if(cache!=null){
			return cache.getNativeCache().getKeys();
		}
		return new ArrayList<String>();
	}

	@Override
	public <T> List<T> getValueList(String cacheName) throws Exception {
		// TODO Auto-generated method stub
		List<T> valueList = new ArrayList<T>();
		for(String key : this.getKeyList(cacheName)){
			valueList.add((T) this.getValue(cacheName, key));
		}
		return valueList;
	}

	@Override
	protected void removeValue(String cacheName, String key) throws Exception {
		// TODO Auto-generated method stub
		EhCacheCache cache = (EhCacheCache)cacheManager.getCache(cacheName);
		if(cache!=null){
			cache.getNativeCache().remove(key);
		}
	}

	private Integer getCacheInMemory(){
		if(cacheInMemory==null || "".equals(cacheInMemory)){
			return 100;
		}else{
			return new Integer(cacheInMemory);
		}
	}
}
