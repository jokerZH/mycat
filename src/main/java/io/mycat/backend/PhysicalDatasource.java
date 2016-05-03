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

import io.mycat.backend.heartbeat.DBHeartbeat;
import io.mycat.net.NetSystem;
import io.mycat.server.Alarms;
import io.mycat.server.config.node.DBHostConfig;
import io.mycat.server.config.node.DataHostConfig;
import io.mycat.server.executors.ConnectionHeartBeatHandler;
import io.mycat.server.executors.DelegateResponseHandler;
import io.mycat.server.executors.NewConnectionRespHandler;
import io.mycat.server.executors.ResponseHandler;
import io.mycat.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/* 保存一个mysql实例的连接 */
public abstract class PhysicalDatasource {
    public static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDatasource.class);
    private final DBHostConfig config;          /* mysql实力的配置 */

    private final String name;                  /* 节点名 */
    private final int size;                     /* 连接池的大小 */
    private final ConMap conMap = new ConMap(); /* 保存当前mysql实例的所有连接 */
    private DBHeartbeat heartbeat;              /* TODO 负责心跳 */
    private final boolean readNode;             /* 是否是读节点 */
    private volatile long heartbeatRecoveryTime;/* 两次心跳之间的最小间隔 */
    private final DataHostConfig hostConfig;    /* TODO */
    private final ConnectionHeartBeatHandler conHeartBeatHanler = new ConnectionHeartBeatHandler(); /* 负责具体的心跳检测 */
    private PhysicalDBPool dbPool;              /* 所属的slice */

    public PhysicalDatasource(DBHostConfig config, DataHostConfig hostConfig, boolean isReadNode) {
        this.size = config.getMaxCon();
        this.config = config;
        this.name = config.getHostName();
        this.hostConfig = hostConfig;
        heartbeat = this.createHeartBeat();
        this.readNode = isReadNode;
    }

    /* 判断连接是否是本mysql实例的 */
    public boolean isMyConnection(BackendConnection con) { return (con.getPool() == this); }

    public DataHostConfig getHostConfig() { return hostConfig; }
    public boolean isReadNode() { return readNode; }
    public int getSize() { return size; }
    public void setDbPool(PhysicalDBPool dbPool) { this.dbPool = dbPool; }
    public PhysicalDBPool getDbPool() { return dbPool; }
    public String getName() { return name; }
    public long getExecuteCountForSchema(String schema) { return conMap.getSchemaConQueue(schema).getExecuteCount(); }
    public int getActiveCountForSchema(String schema) { return conMap.getActiveCountForSchema(schema, this); }
    public DBHeartbeat getHeartbeat() { return heartbeat; }


    public abstract DBHeartbeat createHeartBeat();

    /* 获得当前节点在slice的写节点中的下标, 不在里面也返回0 */
    public int getIndex(){
        int currentIndex = 0;
        for(int i=0;i<dbPool.getSources().length;i++){
            PhysicalDatasource writeHostDatasource = dbPool.getSources()[i];
            if(writeHostDatasource.getName().equals(getName())){
                currentIndex = i;
                break;
            }
        }
        return currentIndex;
    }

    /* 判断当前节点是否可读 */
    public boolean isSalveOrRead(){
        int currentIndex = getIndex();
        if(currentIndex!=dbPool.activedIndex ||this.readNode ){
            return true;
        }
        return false;
    }

    /* 获得当前mysql实例的连接个数 */
    public long getExecuteCount() {
        long executeCount = 0;
        for (ConQueue queue : conMap.getAllConQueue()) {
            executeCount += queue.getExecuteCount();
        }
        return executeCount;
    }


    /* 获得当前mysql实例下某个物理db的空闲连接 */
    public int getIdleCountForSchema(String schema) {
        ConQueue queue = conMap.getSchemaConQueue(schema);
        int total = 0;
        total += queue.getAutoCommitCons().size() + queue.getManCommitCons().size();
        return total;
    }

    /* 获得当前mysql实例的空闲连接 */
    public int getIdleCount() {
        int total = 0;
        for (ConQueue queue : conMap.getAllConQueue()) {
            total += queue.getAutoCommitCons().size() + queue.getManCommitCons().size();
        }
        return total;
    }

    /* TODO */
    private boolean validSchema(String schema) {
        String theSchema = schema;
        return theSchema != null & !"".equals(theSchema) && !"snyn...".equals(theSchema);
    }

    /* 从checkLis中找出需要心跳检测的连接 */
    private void checkIfNeedHeartBeat(
            LinkedList<BackendConnection> heartBeatCons,    /* 返回需要心跳检测的连接 */
            ConQueue queue,
            ConcurrentLinkedQueue<BackendConnection> checkLis,  /* 传入的可能需要心跳检测的连接 */
            long hearBeatTime,  /* 空闲不超过这个时间就不发心跳 */
            long hearBeatTime2
    ) {
        int maxConsInOneCheck = 10;
        Iterator<BackendConnection> checkListItor = checkLis.iterator();
        while (checkListItor.hasNext()) {
            BackendConnection con = checkListItor.next();
            if (con.isClosedOrQuit()) {
                // 如果后端连接关闭了
                checkListItor.remove();
                continue;
            }

            if (validSchema(con.getSchema())) {
                if (con.getLastTime() < hearBeatTime && heartBeatCons.size() < maxConsInOneCheck) {
                    checkListItor.remove();
                    con.setBorrowed(true);
                    // Heart beat check
                    heartBeatCons.add(con);
                }
            } else if (con.getLastTime() < hearBeatTime2) {
                // not valid schema conntion should close for idle
                // exceed 2*conHeartBeatPeriod
                checkListItor.remove();
                con.close(" heart beate idle ");
            }
        }
    }

    /* 发起心跳 */
    public void heatBeatCheck(long timeout, long conHeartBeatPeriod) {
        int ildeCloseCount = hostConfig.getMinCon() * 3;
        int maxConsInOneCheck = 5;
        LinkedList<BackendConnection> heartBeatCons = new LinkedList<BackendConnection>();

        long hearBeatTime = TimeUtil.currentTimeMillis() - conHeartBeatPeriod;
        long hearBeatTime2 = TimeUtil.currentTimeMillis() - 2 * conHeartBeatPeriod;

        /* 找到需要心跳检测的连接 */
        for (ConQueue queue : conMap.getAllConQueue()) {
            checkIfNeedHeartBeat(heartBeatCons, queue, queue.getAutoCommitCons(), hearBeatTime, hearBeatTime2);
            if (heartBeatCons.size() < maxConsInOneCheck) {
                checkIfNeedHeartBeat(heartBeatCons, queue, queue.getManCommitCons(), hearBeatTime, hearBeatTime2);
            } else if (heartBeatCons.size() >= maxConsInOneCheck) {
                break;
            }
        }

        // 发送请求
        if (!heartBeatCons.isEmpty()) {
            for (BackendConnection con : heartBeatCons) {
                conHeartBeatHanler.doHeartBeat(con, hostConfig.getHeartbeatSQL());
            }
        }

        // 关闭心跳超时的连接
        conHeartBeatHanler.abandTimeOuttedConns();

        int idleCons = getIdleCount();
        int activeCons = this.getActiveCount();
        int createCount = (hostConfig.getMinCon() - idleCons) / 3;
        if ((createCount > 0) && (idleCons + activeCons < size) && (idleCons < hostConfig.getMinCon())) {
            // 如果空闲连接太少,则创建
            createByIdleLitte(idleCons, createCount);

        } else if (idleCons > hostConfig.getMinCon()) {
            // 如果空闲连接太多,则close
            closeByIdleMany(idleCons-hostConfig.getMinCon());

        } else {
            int activeCount = this.getActiveCount();
            if (activeCount > size) {
                StringBuilder s = new StringBuilder();
                s.append(Alarms.DEFAULT).append("DATASOURCE EXCEED [name=")
                        .append(name).append(",active=");
                s.append(activeCount).append(",size=").append(size).append(']');
                LOGGER.warn(s.toString());
            }
        }
    }

    /* 关闭idleCloseCount个连接 */
    private void closeByIdleMany(int ildeCloseCount) {
        LOGGER.info("too many ilde cons ,close some for datasouce  " + name);

        List<BackendConnection> readyCloseCons = new ArrayList<BackendConnection>(ildeCloseCount);
        for (ConQueue queue : conMap.getAllConQueue()) {
            readyCloseCons.addAll(queue.getIdleConsToClose(ildeCloseCount));
            if (readyCloseCons.size() >= ildeCloseCount) {
                break;
            }
        }

        for (BackendConnection idleCon : readyCloseCons) {
            if (idleCon.isBorrowed()) {
                LOGGER.warn("find idle con is using " + idleCon);
            }
            idleCon.close("too many idle con");
        }
    }

    /* 新建一些连接 */
    private void createByIdleLitte(int idleCons, int createCount) {
        LOGGER.info("create connections ,because idle connection not enough ,cur is " + idleCons + ", minCon is " + hostConfig.getMinCon() + " for " + name);

        NewConnectionRespHandler simpleHandler = new NewConnectionRespHandler();

        final String[] schemas = dbPool.getSchemas();
        for (int i = 0; i < createCount; i++) {
            if (this.getActiveCount() + this.getIdleCount() >= size) {
                break;
            }
            try {
                // 创建新的连接, 异步的
                this.createNewConnection(simpleHandler, null, schemas[i%schemas.length]);
            } catch (IOException e) {
                LOGGER.warn("create connection err " + e);
            }
        }
    }

    public int getActiveCount() { return this.conMap.getActiveCountForDs(this); }
    public void clearCons(String reason) { this.conMap.clearConnections(reason, this); }

    public void startHeartbeat() { heartbeat.start(); }
    public void stopHeartbeat() { heartbeat.stop(); }
    public void doHeartbeat() {
        // 未到预定恢复时间，不执行心跳检测。
        if (TimeUtil.currentTimeMillis() < heartbeatRecoveryTime) {
            return;
        }

        if (!heartbeat.isStop()) {
            try {
                heartbeat.heartbeat();
            } catch (Exception e) {
                LOGGER.error(name + " heartbeat error.", e);
            }
        }
    }

    /*
     * 后端连接建立之后,放入连接池
     * 或者获得一个连接的时候会调用
     */
    private BackendConnection takeCon(BackendConnection conn,
                                      final ResponseHandler handler, final Object attachment,
                                      String schema) {

        conn.setBorrowed(true);
        if (!conn.getSchema().equals(schema)) {
            // need do schema syn in before sql send
            conn.setSchema(schema);
        }
        ConQueue queue = conMap.getSchemaConQueue(schema);
        queue.incExecuteCount();
        conn.setAttachment(attachment);
        // 每次取连接的时候，更新下lasttime，防止在前端连接检查的时候，关闭连接，导致sql执行失败
        conn.setLastTime(System.currentTimeMillis());
        // handler是NewConnectionRespHandler, 这里会释放Conn,使他返回到连接池子中
        handler.connectionAcquired(conn);
        return conn;
    }

    /* 创建新的连接 */
    private void createNewConnection(final ResponseHandler handler, final Object attachment, final String schema) throws IOException {
        // 异步创建连接
        NetSystem.getInstance().getExecutor().execute(new Runnable() {
            public void run() {
                try {
                    createNewConnection(
                        new DelegateResponseHandler(handler) {
                            @Override
                            public void connectionError(Throwable e, BackendConnection conn) {
                                handler.connectionError(e, conn);
                            }

                            @Override
                            public void connectionAcquired(BackendConnection conn) {
                                takeCon(conn, handler, attachment, schema);
                            }
                        },
                       schema
                    );
                } catch (IOException e) {
                    handler.connectionError(e, null);
                }
            }
        });
    }

    /* 获得一个连接, 连接建立后会调用handler中的函数 */
    public void getConnection(String schema, boolean autocommit, final ResponseHandler handler, final Object attachment) throws IOException {
        BackendConnection con = this.conMap.tryTakeCon(schema, autocommit);
        if (con != null) {
            takeCon(con, handler, attachment, schema);
            return;
        } else {
            int activeCons = this.getActiveCount();//当前最大活动连接
            if(activeCons+1>size){//下一个连接大于最大连接数
                LOGGER.error("the max activeConnnections size can not be max than maxconnections");
                throw new IOException("the max activeConnnections size can not be max than maxconnections");
            }else{            // create connection
                LOGGER.info("not ilde connection in pool,create new connection for " + this.name + " of schema "+schema);
                createNewConnection(handler, attachment, schema);
            }
        }

    }

    /* 将连接放入连接池中 */
    private void returnCon(BackendConnection c) {
        c.setAttachment(null);
        c.setBorrowed(false);
        c.setLastTime(TimeUtil.currentTimeMillis());
        ConQueue queue = this.conMap.getSchemaConQueue(c.getSchema());

        boolean ok = false;
        if (c.isAutocommit()) {
            ok = queue.getAutoCommitCons().offer(c);
        } else {
            ok = queue.getManCommitCons().offer(c);
        }
        if (!ok) {
            LOGGER.warn("can't return to pool ,so close con " + c);
            c.close("can't return to pool ");
        }
    }

    /* 将连接放入连接池中 */
    public void releaseChannel(BackendConnection c) {
        returnCon(c);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("release channel " + c);
        }
    }

    /* 将连接从连接池中移除 */
    public void connectionClosed(BackendConnection conn) {
        ConQueue queue = this.conMap.getSchemaConQueue(conn.getSchema());
        if (queue != null) {
            queue.removeCon(conn);
        }
    }

    /* 创建新的连接 */
    public abstract void createNewConnection(ResponseHandler handler, String schema) throws IOException;

    public long getHeartbeatRecoveryTime() {
        return heartbeatRecoveryTime;
    }

    public void setHeartbeatRecoveryTime(long heartbeatRecoveryTime) {
        this.heartbeatRecoveryTime = heartbeatRecoveryTime;
    }

    public DBHostConfig getConfig() {
        return config;
    }
}
