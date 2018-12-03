package com.gemo.core.cache;

import java.util.List;

public interface EMSCache {

	public void put(String cacheName,String key,Object value) throws Exception;
	
	public <T> T get(String cacheName,String key) throws Exception;
	
	public void createCache(String cacheName);
	
	public List<String> getKeyList(String cacheName) throws Exception;
	
	public <T> List<T> getValueList(String cacheName) throws Exception;
	
	public void remove(String cacheName,String key) throws Exception;
}
