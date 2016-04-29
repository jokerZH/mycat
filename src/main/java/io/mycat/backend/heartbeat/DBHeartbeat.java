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
package io.mycat.backend.heartbeat;

import io.mycat.backend.HeartbeatRecorder;
import java.util.concurrent.atomic.AtomicBoolean;


public abstract class DBHeartbeat {
	public static final int DB_SYN_ERROR = -1;
	public static final int DB_SYN_NORMAL = 1;

	public static final int OK_STATUS = 1;
	public static final int ERROR_STATUS = -1;
	public static final int TIMEOUT_STATUS = -2;
	public static final int INIT_STATUS = 0;

	private static final long DEFAULT_HEARTBEAT_TIMEOUT = 30 * 1000L;	/* 30s */
	private static final int DEFAULT_HEARTBEAT_RETRY = 10;				/* 10次 */

	// heartbeat config
	protected long 	heartbeatTimeout = DEFAULT_HEARTBEAT_TIMEOUT;	/* 心跳超时时间 */
	protected int 	heartbeatRetry = DEFAULT_HEARTBEAT_RETRY; 		/* 检查连接发生异常到切换，重试次数 */
	protected String heartbeatSQL;									/* 静态心跳语句 */

	protected final AtomicBoolean isStop = new AtomicBoolean(true);			/* TODO */
	protected final AtomicBoolean isChecking = new AtomicBoolean(false); 	/* TODO */
	protected int errorCount;			/* TODO */
	protected volatile int status;		/* 当前的状态 */
	protected final HeartbeatRecorder recorder = new HeartbeatRecorder(); 	/* TODO */

	private volatile Integer slaveBehindMaster;
	private volatile int dbSynStatus = DB_SYN_NORMAL;

	public Integer getSlaveBehindMaster() { return slaveBehindMaster; }
	public void setSlaveBehindMaster(Integer slaveBehindMaster) { this.slaveBehindMaster = slaveBehindMaster; }
	public int getDbSynStatus() { return dbSynStatus; }
	public void setDbSynStatus(int dbSynStatus) { this.dbSynStatus = dbSynStatus; }
	public int getStatus() { return status; }
	public boolean isChecking() { return isChecking.get(); }
	public boolean isStop() { return isStop.get(); }
	public int getErrorCount() { return errorCount; }
	public HeartbeatRecorder getRecorder() { return recorder; }
	public long getHeartbeatTimeout() { return heartbeatTimeout; }
	public void setHeartbeatTimeout(long heartbeatTimeout) { this.heartbeatTimeout = heartbeatTimeout; }
	public int getHeartbeatRetry() { return heartbeatRetry; }
	public void setHeartbeatRetry(int heartbeatRetry) { this.heartbeatRetry = heartbeatRetry; }
	public String getHeartbeatSQL() { return heartbeatSQL; }
	public void setHeartbeatSQL(String heartbeatSQL) { this.heartbeatSQL = heartbeatSQL; }
	public boolean isNeedHeartbeat() { return heartbeatSQL != null; }


	/* TODO 开始心跳 */
	public abstract void start();

	/* TODO 停止心跳 */
	public abstract void stop();

	/* TODO 获得上次心跳时间 */
	public abstract String getLastActiveTime();

	/* TODO 获得超时时间 */
	public abstract long getTimeout();

	/* 做一次心跳 */
	public abstract void heartbeat();
}