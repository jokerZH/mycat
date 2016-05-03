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

import io.mycat.route.RouteResultsetNode;
import io.mycat.server.executors.ResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* 相当于一个slice */
public class PhysicalDBNode {
	protected static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDBNode.class);

	protected final String name;			/* TODO sliceName*/
	protected final String database;		/* TODO 物理db名字 */
	protected final PhysicalDBPool dbPool;	/* 保存本slice所在mysql实例的连接, 所以,多个schema是共享连接的 */

	public PhysicalDBNode(String hostName, String database, PhysicalDBPool dbPool) {
		this.name = hostName;
		this.database = database;
		this.dbPool = dbPool;
	}
	public String getName() { return name; }
	public PhysicalDBPool getDbPool() { return dbPool; }
	public String getDatabase() { return database; }

	/* 从连接池中获得连接 */
	public void getConnectionFromSameSource(
			String schema,				/* 物理dbName */
			boolean autocommit,			/* 是否自动提交 */
			BackendConnection exitsCon,	/* 参照的后端连接 */
			ResponseHandler handler,	/* 处理数据需要 */
			Object attachment			/* TODO */
	) throws Exception {

		PhysicalDatasource ds = this.dbPool.findDatasouce(exitsCon);
		if (ds == null) {
			throw new RuntimeException("can't find exits connection,maybe fininshed " + exitsCon);
		} else {
			ds.getConnection(schema,autocommit, handler, attachment);
		}
	}

	/* 检测参数schema是否和当前的database相同,db池子是否初始化成功  */
	private void checkRequest(String schema){
		if (schema != null && !schema.equals(this.database)) {
			throw new RuntimeException("invalid param ,connection request db is :" + schema + " and datanode db is " + this.database);
		}

		if (!dbPool.isInitSuccess()) {
			dbPool.init(dbPool.activedIndex);
		}
	}

	/* 获得一个后段连接  */
	public void getConnection(
			String schema,				/* 物理dbName  */
			boolean autoCommit,			/* 是否自动提交 */
			RouteResultsetNode rrs,		/* 路由结果 */
			ResponseHandler handler,	/* 回应的处理方式 */
			Object attachment			/* TODO */
	) throws Exception {
		checkRequest(schema);

		if (dbPool.isInitSuccess()) {
			if (rrs.canRunnINReadDB(autoCommit)) {
				/* 获得读或者写 */
				dbPool.getRWBanlanceCon(schema, autoCommit, handler, attachment, this.database);
			} else {
				/* 获得一个写 */
				dbPool.getSource().getConnection(schema,autoCommit, handler, attachment);
			}

		} else {
			throw new IllegalArgumentException("Invalid DataSource:" + dbPool.getActivedIndex());
		}
	}
}