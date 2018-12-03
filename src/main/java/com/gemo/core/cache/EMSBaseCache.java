package com.gemo.core.cache;

import java.util.HashMap;
import java.util.Map;

public abstract class EMSBaseCache implements EMSCache {

	protected static final Map<String,Class<?>> cacheKeyMap = new HashMap<String,Class<?>>();
	
	@Override
	public void put(String cacheName, String key, Object value) throws Exception {
		// TODO Auto-generated method stub
		check(cacheName,key);
		putValue(cacheName,key,value);
	}

	@Override
	public <T> T get(String cacheName, String key) throws Exception {
		// TODO Auto-generated method stub
		check(cacheName,key);
		return getValue(cacheName,key);
	}

	@Override
	public void remove(String cacheName, String key) throws Exception {
		// TODO Auto-generated method stub
		check(cacheName,key);
		removeValue(cacheName,key);
	}

	private void check(String cacheName, String key) throws Exception {
		Class<?> cls = cacheKeyMap.get(cacheName);
		if(cls!=null && cls!=key.getClass()){
			throw new Exception("缓存键类型不匹配！");
		}
	}
	
	protected abstract void putValue(String cacheName, String key, Object value) throws Exception;
	
	protected abstract <T> T getValue(String cacheName, String key) throws Exception;
	
	protected abstract void removeValue(String cacheName, String key) throws Exception;

}
