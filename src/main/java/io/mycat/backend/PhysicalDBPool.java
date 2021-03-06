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
package io.mycat.backend;

import io.mycat.MycatServer;
import io.mycat.backend.heartbeat.DBHeartbeat;
import io.mycat.server.Alarms;
import io.mycat.server.config.node.DataHostConfig;
import io.mycat.server.executors.GetConnectionHandler;
import io.mycat.server.executors.ResponseHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/* 对应具有主从关系的多个mysql实例的连接池,所以多个slice可能共用一个POOL */
public class PhysicalDBPool {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDBPool.class);
    private final Random random = new Random();
    private final Random wnrandom = new Random();
    protected final ReentrantLock switchLock = new ReentrantLock();

    /* balance */
    public static final int BALANCE_NONE = 0;
    public static final int BALANCE_ALL_BACK = 1;
    public static final int BALANCE_ALL = 2;
    public static final int BALANCE_ALL_READ = 3;

    /* write */
    public static final int WRITE_ONLYONE_NODE = 0;     /* 只写一个写mysql实例 */
    public static final int WRITE_RANDOM_NODE = 1;      /* 所以选择一个mysql写 */
    public static final int WRITE_ALL_NODE = 2;

    /* other */
    public static final long LONG_TIME = 300000;    /* 5min */
    public static final int WEIGHT = 0;

    private final String hostName;                              /* 这个mysql实例集合的名字 */
    protected PhysicalDatasource[] writeSources;	            /* 写节点 */
    protected Map<Integer, PhysicalDatasource[]> readSources;	/* 读节点 */
    protected volatile int activedIndex;	                    /* 当是WRITE_ONLYONE_NODE模式的时候,表示写哪个mysql实例 */
    protected volatile boolean initSuccess;                     /* 是否初始化成功 */
    private final Collection<PhysicalDatasource> allDs;	        /* 所有的后段节点 */
    private final int banlance;                         /* banlance的模式 */
    private final int writeType;                        /* 选择写实例的模式 :*/
    private String[] schemas;	                        /* 当前mysql实例上有那些物理dbName */

    private final DataHostConfig dataHostConfig;    /* config */

    public PhysicalDBPool(
            String name,
            DataHostConfig conf,
            PhysicalDatasource[] writeSources,
            Map<Integer, PhysicalDatasource[]> readSources,
            int balance,
            int writeType
    ) {
        this.hostName = name;
        this.dataHostConfig = conf;
        this.writeSources = writeSources;
        this.banlance = balance;
        this.writeType = writeType;
        Iterator<Map.Entry<Integer, PhysicalDatasource[]>> entryItor = readSources.entrySet().iterator();
        while (entryItor.hasNext()) {
            PhysicalDatasource[] values = entryItor.next().getValue();
            if (values.length == 0) {
                entryItor.remove();
            }
        }
        this.readSources = readSources;
        this.allDs = this.genAllDataSources();

        LOGGER.info("total resouces of dataHost " + this.hostName + " is :" + allDs.size());
        setDataSourceProps();
    }
    private void setDataSourceProps() {
        for (PhysicalDatasource ds : this.allDs) { ds.setDbPool(this); }
    }

    public int getWriteType() { return writeType; }
    public int getBalance() { return banlance; }
    public String getHostName() { return hostName; }
    public PhysicalDatasource[] getSources() { return writeSources; }
    public int getActivedIndex() { return activedIndex; }
    public boolean isInitSuccess() { return initSuccess; }


    /* 返回连接所在的mysql实例 */
    public PhysicalDatasource findDatasouce(BackendConnection exitsCon) {

        for (PhysicalDatasource ds : this.allDs) {
            if (ds.isReadNode() == exitsCon.isFromSlaveDB()) {
                if (ds.isMyConnection(exitsCon)) {
                    return ds;
                }
            }
        }
        LOGGER.warn("can't find connection in pool " + this.hostName + " con:" + exitsCon);
        return null;
    }


    /* 返回接下来会用的写mysql实例 */
    public PhysicalDatasource getSource() {
        switch (writeType) {
            case WRITE_ONLYONE_NODE: {
                return writeSources[activedIndex];
            }
            case WRITE_RANDOM_NODE: {

                int index = Math.abs(wnrandom.nextInt()) % writeSources.length;
                PhysicalDatasource result = writeSources[index];
                if (!this.isAlive(result)) {
                    // mysql实例不是ok的,则在健康的mysql中选择一个
                    ArrayList<Integer> alives = new ArrayList<Integer>(writeSources.length - 1);
                    for (int i = 0; i < writeSources.length; i++) {
                        if (i != index) {
                            if (this.isAlive(writeSources[i])) {
                                alives.add(i);
                            }
                        }
                    }
                    if (alives.isEmpty()) {
                        result = writeSources[0];
                    } else {
                        index = Math.abs(wnrandom.nextInt()) % alives.size();
                        result = writeSources[alives.get(index)];
                    }
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("select write source " + result.getName() + " for dataHost:" + this.getHostName());
                }
                return result;
            }
            default: {
                throw new java.lang.IllegalArgumentException("writeType is " + writeType + " ,so can't return one write datasource ");
            }
        }
    }

    /* 是否存在i下一个 写的mysql实例 */
    public int next(int i) {
        if (checkIndex(i)) {
            return (++i == writeSources.length) ? 0 : i;
        } else {
            return 0;
        }
    }

    /* 将写mysql给成newIndex */
    public boolean switchSource(int newIndex, boolean isAlarm, String reason) {
        /* 只适用于WRITE_ONLYONE_NODE模式 */
        if (this.writeType != PhysicalDBPool.WRITE_ONLYONE_NODE || !checkIndex(newIndex)) {
            return false;
        }

        final ReentrantLock lock = this.switchLock;
        lock.lock();
        try {
            int current = activedIndex;
            if (current != newIndex) {
                // switch index
                activedIndex = newIndex;
                // init again
                this.init(activedIndex);
                // clear all connections
                this.getSources()[current].clearCons("switch datasource");
                // write log
                LOGGER.warn(switchMessage(current, newIndex, false, reason));
                return true;
            }
        } finally {
            lock.unlock();
        }

        return false;
    }

    /* 构建写实例切换时的日志信息 */
    private String switchMessage(int current, int newIndex, boolean alarm, String reason) {
        StringBuilder s = new StringBuilder();
        if (alarm) {
            s.append(Alarms.DATANODE_SWITCH);
        }
        s.append("[Host=").append(hostName).append(",result=[").append(current).append("->");
        s.append(newIndex).append("],reason=").append(reason).append(']');
        return s.toString();
    }

    private int loop(int i) {
        return i < writeSources.length ? i : (i - writeSources.length);
    }

    /* 从index开始,尝试初始化一个mysql实例的连接池 */
    public void init(int index) {
        if (!checkIndex(index)) {
            index = 0;
        }

        int active = -1;
        for (int i = 0; i < writeSources.length; i++) {
            int j = loop(i + index);
            if (initSource(j, writeSources[j])) {
                //不切换-1时
                if(dataHostConfig.getSwitchType()==DataHostConfig.NOT_SWITCH_DS && j>0)
                {
                    break;
                }
                active = j;
                activedIndex = active;
                initSuccess = true;
                LOGGER.info(getMessage(active, " init success"));

                if (this.writeType == WRITE_ONLYONE_NODE) {
                    // only init one write datasource
                    MycatServer.getInstance().saveDataHostIndex(hostName, activedIndex);
                    break;
                }
            }
        }
        if (!checkIndex(active)) {
            initSuccess = false;
            StringBuilder s = new StringBuilder();
            s.append(Alarms.DEFAULT).append(hostName).append(" init failure");
            LOGGER.error(s.toString());
        }
    }

    /* 判断i是否在写list的范围内 */
    private boolean checkIndex(int i) {
        return i >= 0 && i < writeSources.length;
    }

    private String getMessage(int index, String info) {
        return new StringBuilder().append(hostName).append(" index:").append(index).append(info).toString();
    }

    /* 初始化某一个mysql实例的连接池, 同步等待的方式 */
    private boolean initSource(int index, PhysicalDatasource ds) {
        int initSize = ds.getConfig().getMinCon();
        LOGGER.info("init backend myqsl source ,create connections total " + initSize + " for " + ds.getName() + " index :" + index);

        CopyOnWriteArrayList<BackendConnection> list = new CopyOnWriteArrayList<BackendConnection>();
        GetConnectionHandler getConHandler = new GetConnectionHandler(list, initSize);

        for (int i = 0; i < initSize; i++) {
            try {
                ds.getConnection(this.schemas[i % schemas.length], true, getConHandler, null);
            } catch (Exception e) {
                LOGGER.warn(getMessage(index, " init connection error."), e);
            }
        }

        // waiting for finish
        long timeOut = System.currentTimeMillis() + 60 * 1000;  //等待60s
        while (!getConHandler.finished() && (System.currentTimeMillis() < timeOut)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOGGER.error("initError", e);
            }
        }
        LOGGER.info("init result :" + getConHandler.getStatusInfo());
        return !list.isEmpty();
    }

    /* 对所有后段连接,做一次心跳 */
    public void doHeartbeat() {
        if (writeSources == null || writeSources.length == 0) {
            return;
        }
        
        for (PhysicalDatasource source : this.allDs) {
            if (source != null) {
                source.doHeartbeat();
            } else {
                StringBuilder s = new StringBuilder();
                s.append(Alarms.DEFAULT).append(hostName).append(" current dataSource is null!");
                LOGGER.error(s.toString());
            }
        }
    }

    /**
     * back physical connection heartbeat check
     */
    public void heartbeatCheck(long ildCheckPeriod) {
        for (PhysicalDatasource ds : allDs) {
            // only readnode or all write node or writetype=WRITE_ONLYONE_NODE
            // and current write node will check
            if (
                    ds != null
                    && (ds.getHeartbeat().getStatus() == DBHeartbeat.OK_STATUS)
                    && (
                            ds.isReadNode()
                            || (this.writeType != WRITE_ONLYONE_NODE)
                            || (this.writeType == WRITE_ONLYONE_NODE && ds == this.getSource())
                        )
               )
            {
                ds.heatBeatCheck(ds.getConfig().getIdleTimeout(), ildCheckPeriod);
            }
        }
    }

    public void startHeartbeat() {
        for (PhysicalDatasource source : this.allDs) {
            source.startHeartbeat();
        }
    }

    public void stopHeartbeat() {
        for (PhysicalDatasource source : this.allDs) {
            source.stopHeartbeat();
        }
    }

    public void clearDataSources(String reason) {
        LOGGER.info("clear datasours of pool " + this.hostName);
        for (PhysicalDatasource source : this.allDs) {
            LOGGER.info("clear datasoure of pool  " + this.hostName + " ds:" + source.getConfig());
            source.clearCons(reason);
            source.stopHeartbeat();
        }
    }

    /* 归总所有后端节点 */
    public Collection<PhysicalDatasource> genAllDataSources() {
        LinkedList<PhysicalDatasource> allSources = new LinkedList<PhysicalDatasource>();
        for (PhysicalDatasource ds : writeSources) {
            if (ds != null) {
                allSources.add(ds);
            }
        }
        for (PhysicalDatasource[] dataSources : this.readSources.values()) {
            for (PhysicalDatasource ds : dataSources) {
                if (ds != null) {
                    allSources.add(ds);
                }
            }
        }
        return allSources;
    }

    public Collection<PhysicalDatasource> getAllDataSources() {
        return this.allDs;
    }

    /* 获得读连接 */
    public void getRWBanlanceCon(String schema, boolean autocommit, ResponseHandler handler, Object attachment, String database) throws Exception {
        PhysicalDatasource theNode = null;
        ArrayList<PhysicalDatasource> okSources = null;
        switch (banlance) {
            case BALANCE_ALL_BACK: {
                // all read nodes and the standard by masters ,除了当前写的master
                okSources = getAllActiveRWSources(true, false, checkSlaveSynStatus());
                if (okSources.isEmpty()) {
                    theNode = this.getSource();
                } else {
                    theNode = randomSelect(okSources);
                }
                break;
            }
            case BALANCE_ALL: {
                okSources = getAllActiveRWSources(true, true, checkSlaveSynStatus());
                theNode = randomSelect(okSources);
                break;
            }
            case BALANCE_ALL_READ: {
                okSources = getAllActiveRWSources(false, false, checkSlaveSynStatus());
                theNode = randomSelect(okSources);
                break;
            }
            case BALANCE_NONE:
            default:
                // return default write data source
                theNode = this.getSource();
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("select read source " + theNode.getName() + " for dataHost:" + this.getHostName());
        }

        theNode.getConnection(schema, autocommit, handler, attachment);
    }

    private boolean checkSlaveSynStatus() {
        return (dataHostConfig.getSlaveThreshold() != -1)
                && (dataHostConfig.getSwitchType() == DataHostConfig.SYN_STATUS_SWITCH_DS);
    }

    /**
     * TODO
	 * 随机选择，按权重设置随机概率。
     * 在一个截面上碰撞的概率高，但调用量越大分布越均匀，而且按概率使用权重后也比较均匀，有利于动态调整提供者权重。
	 * @param okSources
	 * @return
	 */
	public PhysicalDatasource randomSelect(ArrayList<PhysicalDatasource> okSources) {
		
		if (okSources.isEmpty()) {
			return this.getSource();
			
		} else {		
			
			int length = okSources.size(); 	// 总个数
	        int totalWeight = 0; 			// 总权重
	        boolean sameWeight = true; 		// 权重是否都一样
	        for (int i = 0; i < length; i++) {	        	
	            int weight = okSources.get(i).getConfig().getWeight();
	            totalWeight += weight; 		// 累计总权重	            
	            if (sameWeight && i > 0 
	            		&& weight != okSources.get(i-1).getConfig().getWeight() ) {	  // 计算所有权重是否一样          		            	
	                sameWeight = false; 	
	            }
	        }
	        
	        if (totalWeight > 0 && !sameWeight ) {
	            
	        	// 如果权重不相同且权重大于0则按总权重数随机
	            int offset = random.nextInt(totalWeight);
	            
	            // 并确定随机值落在哪个片断上
	            for (int i = 0; i < length; i++) {
	                offset -= okSources.get(i).getConfig().getWeight();
	                if (offset < 0) {
	                    return okSources.get(i);
	                }
	            }
	        }
	        
	        // 如果权重相同或权重为0则均等随机
	        return okSources.get( random.nextInt(length) );	
	        
			//int index = Math.abs(random.nextInt()) % okSources.size();
			//return okSources.get(index);
		}
	}

    /* 判断mysql实例是否是活的 */
    private boolean isAlive(PhysicalDatasource theSource) {
        return (theSource.getHeartbeat().getStatus() == DBHeartbeat.OK_STATUS);
    }

    /* 判断mysql实例是否可以用于读 */
    private boolean canSelectAsReadNode(PhysicalDatasource theSource) {

        if(theSource.getHeartbeat().getSlaveBehindMaster()==null ||theSource.getHeartbeat().getDbSynStatus()==DBHeartbeat.DB_SYN_ERROR){
            // 如果是master或者同步有问题
            return false;
        }

        return (theSource.getHeartbeat().getDbSynStatus() == DBHeartbeat.DB_SYN_NORMAL) &&
               (theSource.getHeartbeat().getSlaveBehindMaster() < this.dataHostConfig.getSlaveThreshold());
    }

    /**
     * 返回mysql实例
     * @param includeWriteNode      是否包含写连接
     * @param includeCurWriteNode   是否包含当前在用的写连接
     * @param filterWithSlaveThreshold  是否根据slave threshold过滤
     *
     * @return
     */
    private ArrayList<PhysicalDatasource> getAllActiveRWSources(boolean includeWriteNode, boolean includeCurWriteNode, boolean filterWithSlaveThreshold) {
        int curActive = activedIndex;
        ArrayList<PhysicalDatasource> okSources = new ArrayList<PhysicalDatasource>(this.allDs.size());

        for (int i = 0; i < this.writeSources.length; i++) {
            PhysicalDatasource theSource = writeSources[i];
            if (isAlive(theSource)) {
                // write node is active
                if (includeWriteNode) {
	            	if (i == curActive && includeCurWriteNode == false) {
	                    // not include cur active source
	                } else if (filterWithSlaveThreshold) {
	                    if (canSelectAsReadNode(theSource)) {
	                        okSources.add(theSource);
	                    } else {
	                        continue;
	                    }
	                } else {
	                    okSources.add(theSource);
	                }
                }
                if (!readSources.isEmpty()) {
                    // check all slave nodes
                    PhysicalDatasource[] allSlaves = this.readSources.get(i);
                    if (allSlaves != null) {
                        for (PhysicalDatasource slave : allSlaves) {
                            if (isAlive(slave)) {
                                if (filterWithSlaveThreshold) {
                                    if (canSelectAsReadNode(slave)) {
                                        okSources.add(slave);
                                    } else {
                                        continue;
                                    }
                                } else {
                                    okSources.add(slave);
                                }
                            }
                        }
                    }
                }
                
            } else {
			    // 如果写节点不OK, 也要保证临时的读服务正常
				if ( this.dataHostConfig.isTempReadHostAvailable() ) {
				
					if (!readSources.isEmpty()) {
						// check all slave nodes
						PhysicalDatasource[] allSlaves = this.readSources.get(i);
						if (allSlaves != null) {
							for (PhysicalDatasource slave : allSlaves) {
								if (isAlive(slave)) {
									
									if (filterWithSlaveThreshold) {									
										if (canSelectAsReadNode(slave)) {
											okSources.add(slave);
										} else {
											continue;
										}
										
									} else {
										okSources.add(slave);
									}
								}
							}
						}
					}
				}				
			}

        }
        return okSources;
    }

    public String[] getSchemas() { return schemas; }
    public void setSchemas(String[] mySchemas) { this.schemas = mySchemas; }
}
