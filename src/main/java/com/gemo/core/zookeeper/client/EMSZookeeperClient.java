package com.gemo.core.zookeeper.client;

import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gemo.core.utils.CommonUtils;

@Component("EMSZookeeperClient")
public class EMSZookeeperClient {

	private static Logger log = Logger.getLogger(EMSZookeeperClient.class);

	public static final String rootPath = "/EMS/Dictionary";

	private CuratorFramework client;

	@Value("${zookeeper.server.ip}")
	private String zookeeperServerIp;

	@Value("${zookeeper.server.port}")
	private String zookeeperServerPort;

	@Value("${zookeeper.session.timeout}")
	private String zookeeperSessionTimeout;

	@Value("${zookeeper.connection.timeout}")
	private String zookeeperConnectionTimeout;

	private Boolean started = false;

	private String zookeeperClientDomain;

	@PostConstruct
	private void init() {
		if (!started) {
			log.info("连接zookeeper开始.......");
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 6, 10000);
			String connectionString = zookeeperServerIp + ":" + zookeeperServerPort;
			Integer sessionTimeout = new Integer(zookeeperSessionTimeout);
			Integer connectionTimeout = new Integer(zookeeperConnectionTimeout);
			client = CuratorFrameworkFactory.newClient(connectionString, sessionTimeout, connectionTimeout,
					retryPolicy);
			client.start();
			int count = 1;
			while (true) {
				log.info("正在连接zookeeper中.......检查" + count++ + "次！");
				CuratorFrameworkState state = client.getState();
				if (state == CuratorFrameworkState.STARTED) {
					started = true;
					break;
				}
				CommonUtils.sleep(1000L);
			}

			log.info("连接zookeeper成功.......");
		}
	}

	public boolean exists(String path) throws Exception {
		Stat stat = client.checkExists().forPath(path);
		if (stat == null) {
			return false;
		} else {
			return true;
		}
	}

	public void create(String path, byte[] value) throws Exception {
		if (!this.exists(path)) {
			client.create().creatingParentsIfNeeded().forPath(path, value);
		} else {
			throw new Exception("节点已存在！");
		}
	}

	public void update(String path, int version, byte[] value) throws Exception {
		if (this.exists(path)) {
			client.setData().withVersion(version).forPath(path, value);
		} else {
			throw new Exception("节点不存在！");
		}
	}

	public void createOrUpdate(String path, byte[] value) throws Exception {
		if (this.exists(path)) {
			client.setData().forPath(path, value);
		} else {
			client.create().creatingParentsIfNeeded().forPath(path, value);
		}
	}

	public byte[] getData(String path, Stat stat) throws Exception {
		return client.getData().storingStatIn(stat).forPath(path);
	}

	public List<String> getChildern(String path) throws Exception {
		return client.getChildren().forPath(path);
	}

	public void remove(String path) throws Exception {
		client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
	}

	public NodeCache listen(String path, NodeCacheListener nodeCacheListener) throws Exception {
		NodeCache nodeCache = null;
		if (nodeCacheListener != null) {
			nodeCache = new NodeCache(client, path, false);
			nodeCache.start();
			nodeCache.getListenable().addListener(nodeCacheListener);
		}
		return nodeCache;
	}

	public PathChildrenCache listen(String path, PathChildrenCacheListener pathChildrenCacheListener) throws Exception {
		PathChildrenCache pathChildrenCache = null;
		if (pathChildrenCacheListener != null) {
			pathChildrenCache = new PathChildrenCache(client, path, true);
			pathChildrenCache.start();
			pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
		}
		return pathChildrenCache;
	}

	public Boolean getStarted() {
		return started;
	}

	public String getZookeeperClientDomain() {
		return zookeeperClientDomain;
	}

	public void setZookeeperClientDomain(String zookeeperClientDomain) {
		this.zookeeperClientDomain = zookeeperClientDomain;
	}

	public CuratorFramework getClient() {
		return client;
	}

}
