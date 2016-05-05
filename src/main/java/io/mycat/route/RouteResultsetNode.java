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
package io.mycat.route;

import io.mycat.server.parser.ServerParse;
import io.mycat.sqlengine.mpp.LoadData;
import java.io.Serializable;

/* 路由结果 */
public final class RouteResultsetNode implements Serializable , Comparable<RouteResultsetNode> {
	private static final long serialVersionUID = 1L;

	private final String name; 				/* sliceName */
	private String statement; 				/* 后端执行的语句 */
	private final String srcStatement;		/* 客户端传入的sql语句 */
	private final int sqlType;				/* sql类型 */
	private volatile boolean canRunInReadDB;/* 是否可以在read节点上执行,根据sql是否是读请求决定 */
	private final boolean hasBlanceFlag;	/* 如果sql语句有 balance hint就为true */

	private int limitStart;			/* select语句增加的限制,限制结果大小	*/
	private int limitSize;			/* 同上 */
	private int totalNodeSize =0; 	/* 当前routeResult属于的rr集合的rr个数 */

	private LoadData loadData;		// TODO load data in file

	public RouteResultsetNode(String name, int sqlType, String srcStatement) {
		this.name = name;
		this.limitStart=0;
		this.limitSize = -1;
		this.sqlType = sqlType;
		this.srcStatement = srcStatement;
		this.statement = srcStatement;
		canRunInReadDB = (sqlType == ServerParse.SELECT || sqlType == ServerParse.SHOW);
		hasBlanceFlag = (statement != null) && statement.startsWith("/*balance*/");
	}

	/* 判断当前请求,是否可以在slave上运行 */
	public boolean canRunnINReadDB(boolean autocommit) {
		return canRunInReadDB && autocommit && !hasBlanceFlag
			|| canRunInReadDB && !autocommit && hasBlanceFlag;
	}

	public void setStatement(String statement) { this.statement = statement; }
	public void setCanRunInReadDB(boolean canRunInReadDB) { this.canRunInReadDB = canRunInReadDB; }
	public boolean getCanRunInReadDB() { return this.canRunInReadDB; }
	public void resetStatement() { this.statement = srcStatement; }
	public String getName() { return name; }
	public int getSqlType() { return sqlType; }
	public String getStatement() { return statement; }
	public int getLimitStart() { return limitStart; }
	public void setLimitStart(int limitStart) { this.limitStart = limitStart; }
	public int getLimitSize() { return limitSize; }
	public void setLimitSize(int limitSize) { this.limitSize = limitSize; }
	public int getTotalNodeSize() { return totalNodeSize; }
	public void setTotalNodeSize(int totalNodeSize) { this.totalNodeSize = totalNodeSize; }
	public LoadData getLoadData() { return loadData; }
	public void setLoadData(LoadData loadData) { this.loadData = loadData; }
	public boolean isModifySQL() { return !canRunInReadDB; }

	@Override
	public int hashCode() { return name.hashCode(); }

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj instanceof RouteResultsetNode) {
			RouteResultsetNode rrn = (RouteResultsetNode) obj;
			if (equals(name, rrn.getName())) {
				return true;
			}
		}
		return false;
	}

	private static boolean equals(String str1, String str2) {
		if (str1 == null) {
			return str2 == null;
		}
		return str1.equals(str2);
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append(name).append('{').append(statement).append('}');
		return s.toString();
	}

	@Override
	public int compareTo(RouteResultsetNode obj) {
		if(obj == null) {
			return 1;
		}
		if(this.name == null) {
			return -1;
		}
		if(obj.name == null) {
			return 1;
		}
		return this.name.compareTo(obj.name);
	}
}
