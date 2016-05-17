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

import io.mycat.route.util.PageSQLUtil;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.sqlengine.mpp.HavingCols;
import io.mycat.util.FormatUtil;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/* 路由结果集合 */
public final class RouteResultset implements Serializable {
    private String statement;           /* 原始sql语句 */
    private final int sqlType;          /* sql类型*/
    private RouteResultsetNode[] nodes; /* 路由结果节点 */

    private int limitStart;             /* limit start in select sql */
    private int limitSize;              /* limit size in select sql -1 if no limit */

    private String primaryKey;          /* tableName.primerKey */
    private SQLMerge sqlMerge;          /* 感觉跟select的结果合并相关的东西 */

    private boolean cacheAble;                  /* TODO */
    private boolean callStatement = false;      /* TODO 处理call关键字 */
    private boolean globalTableFlag = false;    /* 是否为全局表，只有在insert、update、delete、ddl里会判断并修改。默认不是全局表，用于修正全局表修改数据的反馈 */
    private boolean isFinishedRoute = false;    /* 是否完成了路由 */
    private boolean autocommit = true;          /* 是否自动提交 */
    private boolean isLoadData=false;           /* 是否是load data in file命令 */
    private Boolean canRunInReadDB;             /* 是否可以在从库运行 */


    public RouteResultset(String stmt, int sqlType) {
        this.statement = stmt;
        this.limitSize = -1;
        this.sqlType = sqlType;
    }

    public void resetNodes() {
        if (nodes != null) {
            for (RouteResultsetNode node : nodes) {
                node.resetStatement();
            }
        }
    }

    /* 如果单个routeResult没有配置limitStart limitSize,*/
    public void copyLimitToNodes() {
        if(nodes!=null)
        {
            for (RouteResultsetNode node : nodes)
            {
                if(node.getLimitSize()==-1&&node.getLimitStart()==0)
                {
                    node.setLimitStart(limitStart);
                    node.setLimitSize(limitSize);
                }
            }
        }
    }

    public boolean needMerge() {
        return limitSize > 0 || sqlMerge != null;
    }

    public boolean isHasAggrColumn() {
        return (sqlMerge != null) && sqlMerge.isHasAggrColumn();
    }

    public String[] getGroupByCols() {
        return (sqlMerge != null) ? sqlMerge.getGroupByCols() : null;
    }

    private SQLMerge createSQLMergeIfNull() {
        if (sqlMerge == null) {
            sqlMerge = new SQLMerge();
        }
        return sqlMerge;
    }

    public Map<String, Integer> getMergeCols() {
        return (sqlMerge != null) ? sqlMerge.getMergeCols() : null;
    }

    public void setPrimaryKey(String primaryKey) {
        if (!primaryKey.contains(".")) {
            throw new java.lang.IllegalArgumentException("must be table.primarykey fomat :" + primaryKey);
        }
        this.primaryKey = primaryKey;
    }

    /* return primary key items ,first is table name ,seconds is primary key */
    public String[] getPrimaryKeyItems() { return primaryKey.split("\\."); }

    public void setOrderByCols(LinkedHashMap<String, Integer> orderByCols) {
        if (orderByCols != null && !orderByCols.isEmpty()) {
            createSQLMergeIfNull().setOrderByCols(orderByCols);
        }
    }

    public void setHasAggrColumn(boolean hasAggrColumn) {
        if (hasAggrColumn) {
            createSQLMergeIfNull().setHasAggrColumn(true);
        }
    }

    public void setGroupByCols(String[] groupByCols) {
        if (groupByCols != null && groupByCols.length > 0) {
            createSQLMergeIfNull().setGroupByCols(groupByCols);
        }
    }

    public void setMergeCols(Map<String, Integer> mergeCols) {
        if (mergeCols != null && !mergeCols.isEmpty()) {
            createSQLMergeIfNull().setMergeCols(mergeCols);
        }

    }

    public LinkedHashMap<String, Integer> getOrderByCols() {
        return (sqlMerge != null) ? sqlMerge.getOrderByCols() : null;
    }

    public void setNodes(RouteResultsetNode[] nodes) {
        if(nodes!=null)
        {
           int nodeSize=nodes.length;
            for (RouteResultsetNode node : nodes)
            {
                node.setTotalNodeSize(nodeSize);
            }
        }
        this.nodes = nodes;
    }

    /* 讲routerResult中的sql设置为增加limit之后的sql */
    public void changeNodeSqlAfterAddLimit(
            SchemaConfig schemaConfig,  /* 逻辑db */
            String sourceDbType,        /* 后端类型 */
            String sql,                 /* 增加limit之后的sql */
            int offset,                 /* limit offset */
            int count,                  /* limit count */
            boolean isNeedConvert       /* TODO */
    ) {
        if (nodes != null)
        {
            Map<String, String> dataNodeDbTypeMap = schemaConfig.getDataNodeDbTypeMap();
            Map<String, String> sqlMapCache = new HashMap<>();
            for (RouteResultsetNode node : nodes)
            {
                String dbType = dataNodeDbTypeMap.get(node.getName());
                if (sourceDbType.equalsIgnoreCase("mysql"))
                {
                    //mysql之前已经加好limit
                    node.setStatement(sql);
                } else if (sqlMapCache.containsKey(dbType))
                {
                    node.setStatement(sqlMapCache.get(dbType));
                } else if(isNeedConvert)
                {
                    String nativeSql = PageSQLUtil.convertLimitToNativePageSql(dbType, sql, offset, count);
                    sqlMapCache.put(dbType, nativeSql);
                    node.setStatement(nativeSql);
                }  else {
                    node.setStatement(sql);
                }

                node.setLimitStart(offset);
                node.setLimitSize(count);
            }
        }
    }

	public HavingCols getHavingCols() {
		return (sqlMerge != null) ? sqlMerge.getHavingCols() : null;
	}

	public void setHavings(HavingCols havings) {
		if (havings != null) {
			createSQLMergeIfNull().setHavingCols(havings);
		}
	}


    public boolean isLoadData() { return isLoadData; }
    public void setLoadData(boolean isLoadData)  { this.isLoadData = isLoadData; }
    public boolean isFinishedRoute() { return isFinishedRoute; }
    public void setFinishedRoute(boolean isFinishedRoute) { this.isFinishedRoute = isFinishedRoute; }
    public boolean isGlobalTable() { return globalTableFlag; }
    public void setGlobalTable(boolean globalTableFlag) { this.globalTableFlag = globalTableFlag; }
    public SQLMerge getSqlMerge() { return sqlMerge; }
    public boolean isCacheAble() { return cacheAble; }
    public void setCacheAble(boolean cacheAble) { this.cacheAble = cacheAble; }
    public int getSqlType() { return sqlType; }
    public void setLimitStart(int limitStart) { this.limitStart = limitStart; }
    public int getLimitStart() { return limitStart; }
    public String getPrimaryKey() { return primaryKey; }
    public String getStatement() { return statement; }
    public RouteResultsetNode[] getNodes() { return nodes; }
    public int getLimitSize() { return limitSize; }
    public void setLimitSize(int limitSize) { this.limitSize = limitSize; }
    public void setStatement(String statement) { this.statement = statement; }
    public boolean isCallStatement() { return callStatement; }
    public void setCallStatement(boolean callStatement) { this.callStatement = callStatement; }
    public boolean isAutocommit() { return autocommit; }
    public void setAutocommit(boolean autocommit) { this.autocommit = autocommit; }
    public Boolean getCanRunInReadDB() { return canRunInReadDB; }
    public void setCanRunInReadDB(Boolean canRunInReadDB) { this.canRunInReadDB = canRunInReadDB; }
    public boolean hasPrimaryKeyToCache() { return primaryKey != null; }


    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(statement).append(", route={");
        if (nodes != null) {
            for (int i = 0; i < nodes.length; ++i) {
                s.append("\n ");
                s.append(FormatUtil.format(i + 1, 3)).append(" -> ").append(nodes[i]);
            }
        }
        s.append("\n}");
        return s.toString();
    }
}