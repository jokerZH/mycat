/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.server.config.node;

import io.mycat.MycatServer;
import io.mycat.backend.PhysicalDBNode;
import io.mycat.backend.PhysicalDBPool;
import io.mycat.server.config.cluster.MycatClusterConfig;
import io.mycat.server.config.loader.ConfigInitializer;
import io.mycat.server.config.loader.ConfigReLoader;
import io.mycat.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/* 对应mycat的配置 */
public class MycatConfig implements ConfigReLoader{
	private static final Logger LOGGER = LoggerFactory.getLogger("MycatConfig");
	private static final int RELOAD = 1;
	private static final int ROLLBACK = 2;
    private static final int RELOAD_ALL = 3;
	private final ReentrantLock lock;

	/* system */
	private volatile SystemConfig system;

	/* cluster */
	private volatile MycatClusterConfig cluster;
	private volatile QuarantineConfig quarantine;	/* TODO 集群隔离相关 */

	/* mycat */
	private volatile Map<String/*userName*/, UserConfig> users;				/* 用户信息 */
	private volatile Map<String/*ldbName*/, SchemaConfig> schemas;			/* 逻辑db信息 */
	private volatile Map<String/*dbName*/, PhysicalDBNode> dataNodes;		/* 物理db对应的连接池 */
	private volatile Map<String/*sliceName*/, PhysicalDBPool> dataHosts;	/* 具有主从关系的多个mysql实例 */
	private volatile HostIndexConfig hostIndexConfig;
	private volatile CharsetConfig charsetConfig;
	private volatile SequenceConfig sequenceConfig;

	private long reloadTime;
	private long rollbackTime;
	private int status;

	public MycatConfig() {
		ConfigInitializer confInit = new ConfigInitializer(true);
		this.system = confInit.getSystem();
		this.users = confInit.getUsers();
		this.schemas = confInit.getSchemas();
		this.dataHosts = confInit.getDataHosts();
        this.charsetConfig = confInit.getCharsetConfig();
        this.sequenceConfig = confInit.getSequenceConfig();
        this.hostIndexConfig = confInit.getHostIndexs();

		this.dataNodes = confInit.getDataNodes();
		for (PhysicalDBPool dbPool : dataHosts.values()) {
			dbPool.setSchemas(getDataNodeSchemasOfDataHost(dbPool.getHostName()));
		}
		this.quarantine = confInit.getQuarantine();
		this.cluster = confInit.getCluster();

		this.reloadTime = TimeUtil.currentTimeMillis();
		this.rollbackTime = -1L;
		this.status = RELOAD;
		this.lock = new ReentrantLock();
	}

	private volatile MycatClusterConfig _cluster;
	private volatile QuarantineConfig _quarantine;
	private volatile Map<String, UserConfig> _users;
	private volatile Map<String, SchemaConfig> _schemas;
	private volatile Map<String, PhysicalDBNode> _dataNodes;
	private volatile Map<String, PhysicalDBPool> _dataHosts;
	private volatile HostIndexConfig _hostIndexConfig;
	private volatile CharsetConfig _charsetConfig;
	private volatile SequenceConfig _sequenceConfig;

	public SystemConfig getSystem() { return system; }
	public Map<String, UserConfig> getUsers() { return users; }
	public Map<String, UserConfig> getBackupUsers() { return _users; }
	public Map<String, SchemaConfig> getSchemas() { return schemas; }
	public Map<String, SchemaConfig> getBackupSchemas() { return _schemas; }
	public Map<String, PhysicalDBNode> getDataNodes() { return dataNodes; }
	public Map<String, PhysicalDBNode> getBackupDataNodes() { return _dataNodes; }
	public Map<String, PhysicalDBPool> getDataHosts() { return dataHosts; }
	public Map<String, PhysicalDBPool> getBackupDataHosts() { return _dataHosts; }
	public MycatClusterConfig getCluster() { return cluster; }
	public MycatClusterConfig getBackupCluster() { return _cluster; }
	public QuarantineConfig getQuarantine() { return quarantine; }
	public QuarantineConfig getBackupQuarantine() { return _quarantine; }
	public ReentrantLock getLock() { return lock; }
	public long getReloadTime() { return reloadTime; }
	public long getRollbackTime() { return rollbackTime; }
	public CharsetConfig getBackupCharsetConfig() { return _charsetConfig; }
	public SequenceConfig getBackupSequenceConfig() { return _sequenceConfig; }
	public HostIndexConfig getBackupHostIndexs() { return _hostIndexConfig; }
	public CharsetConfig getCharsetConfig() { return charsetConfig; }
	public void setCharsetConfig(CharsetConfig charsetConfig) { this.charsetConfig = charsetConfig; }
	public SequenceConfig getSequenceConfig() { return sequenceConfig; }
	public void setSequenceConfig(SequenceConfig sequenceConfig) { this.sequenceConfig = sequenceConfig; }

	public String getHostIndex(String hostName, String index) {
		if(this.hostIndexConfig.getProps().isEmpty()
		   || !this.hostIndexConfig.getProps().containsKey(hostName)){
			return index;
		}
		return (String) hostIndexConfig.getProps().get(hostName);
	}
	public void setHostIndex(String hostName, int index) {
		this.hostIndexConfig.getProps().put(hostName, index);
	}

	/* 获得某个slice下所有物理dbName */
	public String[] getDataNodeSchemasOfDataHost(String dataHost) {
		ArrayList<String> schemas = new ArrayList<String>(30);
		for (PhysicalDBNode dn : dataNodes.values()) {
			if (dn.getDbPool().getHostName().equals(dataHost)) {
				schemas.add(dn.getDatabase());
			}
		}
		return schemas.toArray(new String[schemas.size()]);
	}

	public boolean canRollback() {
		if (
				_users == null ||
				_schemas == null ||
				_dataNodes == null ||
				_dataHosts == null ||
				_cluster == null ||
				_quarantine == null ||
				status == ROLLBACK
			)
		{
			return false;
		} else {
			return true;
		}
	}

	public void reload(Map<String, UserConfig> users,
			Map<String, SchemaConfig> schemas,
			Map<String, PhysicalDBNode> dataNodes,
			Map<String, PhysicalDBPool> dataHosts,
			MycatClusterConfig cluster,
			QuarantineConfig quarantine,
			CharsetConfig charsetConfig,
			SequenceConfig sequenceConfig,
			HostIndexConfig hostIndexConfig,boolean reloadAll) {
		apply(users, schemas, dataNodes, dataHosts, cluster, quarantine,charsetConfig,sequenceConfig,hostIndexConfig,reloadAll);
		this.reloadTime = TimeUtil.currentTimeMillis();
		this.status = reloadAll?RELOAD_ALL:RELOAD;
	}

	public void rollback(Map<String, UserConfig> users,
			Map<String, SchemaConfig> schemas,
			Map<String, PhysicalDBNode> dataNodes,
			Map<String, PhysicalDBPool> dataHosts,
			MycatClusterConfig cluster,
			QuarantineConfig quarantine,
			CharsetConfig charsetConfig,
			SequenceConfig sequenceConfig,
			HostIndexConfig hostIndexConfig) {
		apply(users, schemas, dataNodes, dataHosts, cluster, quarantine,charsetConfig,sequenceConfig,hostIndexConfig,status==RELOAD_ALL);
		this.rollbackTime = TimeUtil.currentTimeMillis();
		this.status = ROLLBACK;
	}

	private void apply(Map<String, UserConfig> users,
			Map<String, SchemaConfig> schemas,
			Map<String, PhysicalDBNode> dataNodes,
			Map<String, PhysicalDBPool> dataHosts,
			MycatClusterConfig cluster,
			QuarantineConfig quarantine,
			CharsetConfig charsetConfig,
			SequenceConfig sequenceConfig,
			HostIndexConfig hostIndexConfig,
			boolean isLoadAll) {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
            if(isLoadAll)
            {
                // stop datasource heartbeat
                Map<String, PhysicalDBPool> oldDataHosts = this.dataHosts;
                if (oldDataHosts != null)
                {
                    for (PhysicalDBPool n : oldDataHosts.values())
                    {
                        if (n != null)
                        {
                            n.stopHeartbeat();
                        }
                    }
                }
                this._dataNodes = this.dataNodes;
                this._dataHosts = this.dataHosts;
            }
			this._users = this.users;
			this._schemas = this.schemas;

			this._cluster = this.cluster;
			this._quarantine = this.quarantine;
            this._charsetConfig = this.charsetConfig;
			this._sequenceConfig = this.sequenceConfig;
			this._hostIndexConfig = this.hostIndexConfig;

            if(isLoadAll)
            {
                // start datasoruce heartbeat
                if (dataNodes != null)
                {
                    for (PhysicalDBPool n : dataHosts.values())
                    {
                        if (n != null)
                        {
                            n.startHeartbeat();
                        }
                    }
                }
                this.dataNodes = dataNodes;
                this.dataHosts = dataHosts;
            }
			this.users = users;
			this.schemas = schemas;
			this.cluster = cluster;
			this.quarantine = quarantine;
			this.charsetConfig = charsetConfig;
			this.sequenceConfig = sequenceConfig;
			this.hostIndexConfig = hostIndexConfig;

		} finally {
			lock.unlock();
		}
	}

	/* 初始化mysql实例 */
	public boolean initDatasource(){
		LOGGER.info("Initialize dataHost ...");

		for (PhysicalDBPool node : dataHosts.values()) {
			String index = this.getHostIndex(node.getHostName(),"0");
			if (!"0".equals(index)) {
				LOGGER.info("init datahost: " + node.getHostName() + "  to use datasource index:" + index);
			}
			node.init(Integer.valueOf(index));
			node.startHeartbeat();
		}
		return true;
	}

	/* TODO */
	public boolean reloadDatasource(){
		Map<String, PhysicalDBPool> cNodes = this.getDataHosts();
		boolean reloadStatus = true;
		for (PhysicalDBPool dn : dataHosts.values()) {
			dn.setSchemas(MycatServer.getInstance().getConfig().getDataNodeSchemasOfDataHost(dn.getHostName()));
			//init datahost
             String index =  this.getHostIndex(dn.getHostName(), "0");
			if (!"0".equals(index)) {
				LOGGER.info("init datahost: " + dn.getHostName() + "  to use datasource index:" + index);
			}
			dn.init(Integer.valueOf(index));
			//dn.init(0);
			if (!dn.isInitSuccess()) {
				reloadStatus = false;
				// 如果重载不成功，则清理已初始化的资源。
				LOGGER.warn("reload failed ,clear previously created datasources ");
				dn.clearDataSources("reload config");
				dn.stopHeartbeat();
				break;
			}
		}

		// 处理旧的资源
		for (PhysicalDBPool dn : cNodes.values()) {
			dn.clearDataSources("reload config clear old datasources");
			dn.stopHeartbeat();
		}

		return reloadStatus;
	}

	/* TODO */
	public boolean rebackDatasource(){
		// 如果回滚已经存在的pool
		boolean rollbackStatus = true;
		Map<String, PhysicalDBPool> cNodes = this.getDataHosts();
		for (PhysicalDBPool dn : dataHosts.values()) {
			dn.init(dn.getActivedIndex());
			if (!dn.isInitSuccess()) {
				rollbackStatus = false;
				break;
			}
		}
		// 如果回滚不成功，则清理已初始化的资源。
		if (!rollbackStatus) {
			for (PhysicalDBPool dn : dataHosts.values()) {
				dn.clearDataSources("rollbackup config");
				dn.stopHeartbeat();
			}
			return false;
		}

		// 处理旧的资源
		for (PhysicalDBPool dn : cNodes.values()) {
			dn.clearDataSources("clear old config ");
			dn.stopHeartbeat();
		}
		return rollbackStatus;
	}

	@Override
	public void reloadSchemaConfig(String schema) {}
	@Override
	public void reloadSchemaConfigs() {}
	@Override
	public void reloadDataNodeConfigs() {}
	@Override
	public void reloadDataHostConfigs() {}
	@Override
	public void reloadTableRuleConfigs() {}
	@Override
	public void reloadSystemConfig() {}
	@Override
	public void reloadUserConfig(String user) {}
	@Override
	public void reloadUserConfigs() {}
	@Override
	public void reloadQuarantineConfigs() {}
	@Override
	public void reloadClusterConfigs(){}
	@Override
	public void reloadCharsetConfigs() {}
	@Override
	public void reloadHostIndexConfig() {}
}
