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

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.Set;

import io.mycat.util.SplitUtil;

/* 表示一个逻辑表  */
public class TableConfig {
	private Random rand = new Random();

	public static final int TYPE_GLOBAL_TABLE = 1;
	public static final int TYPE_GLOBAL_DEFAULT = 0;

    private String name;					/* 逻辑表名 */
    private String primaryKey;				/* 主键名 */
    private boolean autoIncrement;			/* 是否自增 */
    private boolean needAddLimit;			/* 是否自动增加ilmit */
    private Set<String> dbTypes;			/* TODO */
    private int tableType;					/* TODO */
    private ArrayList<String> dataNodes;	/* TODO 后端数据接点 */
    private RuleConfig rule;				/* 分表算法 */
	private String ruleName;				/* rule名字 */
    private String partitionColumn;			/* 分表字段名 sharding id */
    private boolean ruleRequired;
    private TableConfig parentTC;			/* 父亲表 */
    private boolean childTable;				/* 是否是子表 */
    private String joinKey;					/* 本表的字段 */
    private String parentKey;				/* joinKey在父亲表中的字段 感觉是外键 joinKey==parentKey */
    private String locateRTableKeySql;		/* TODO 获得root表中的parentKey字段的值 */

    private boolean secondLevel;			/* 是否有父表 */
    private boolean partionKeyIsPrimaryKey;	/* 分表字段是否是主键 */


	public void setPrimaryKey(String primaryKey) { this.primaryKey = primaryKey; }
	public void setAutoIncrement(boolean autoIncrement) { this.autoIncrement = autoIncrement; }
	public void setNeedAddLimit(boolean needAddLimit) { this.needAddLimit = needAddLimit; }
	public void setDbTypes(Set<String> dbTypes) { this.dbTypes = dbTypes; }
	public void setRuleRequired(boolean ruleRequired) { this.ruleRequired = ruleRequired; }
	public void setChildTable(boolean childTable) { this.childTable = childTable; }
	public void setJoinKey(String joinKey) { this.joinKey = joinKey; }
	public void setParentKey(String parentKey) { this.parentKey = parentKey; }
	public String getPrimaryKey() { return primaryKey; }
	public Set<String> getDbTypes() { return dbTypes; }
	public boolean isAutoIncrement() { return autoIncrement; }
	public boolean isNeedAddLimit() { return needAddLimit; }
	public boolean isSecondLevel() { return secondLevel; }
	public String getLocateRTableKeySql() { return locateRTableKeySql; }
	public boolean isGlobalTable() { return this.tableType == TableConfig.TYPE_GLOBAL_TABLE; }
	public String getPartitionColumn() { return partitionColumn; }
	public int getTableType() { return tableType; }
	public TableConfig getParentTC() { return parentTC; }
	public boolean isChildTable() { return childTable; }
	public String getJoinKey() { return joinKey; }
	public String getParentKey() { return parentKey; }
	public String getName() { return name; }
	public ArrayList<String> getDataNodes() { return dataNodes; }
	public boolean isRuleRequired() { return ruleRequired; }
	public RuleConfig getRule() { return rule; }
	public boolean primaryKeyIsPartionKey() { return partionKeyIsPrimaryKey; }
	public String getRuleName() { return ruleName; }
	public void setRuleName(String ruleName) { this.ruleName = ruleName; }


	public void setName(String name) {
        if (name == null) { throw new IllegalArgumentException("table name is null"); }
        this.name = name.toUpperCase();
    }

    public void setTableType(int tableType) {
        this.tableType = TableConfig.TYPE_GLOBAL_DEFAULT == tableType ? TableConfig.TYPE_GLOBAL_DEFAULT : TableConfig.TYPE_GLOBAL_TABLE;
	}

	/* 设置mysql节点名列表,以, $ -分割开 */
    public void setDataNode(String dataNode) {
        if (Strings.isNullOrEmpty(dataNode)) {
            throw new IllegalArgumentException("dataNode name is null");
        }

        String theDataNodes[] = SplitUtil.split(dataNode, ',', '$', '-');

        if (theDataNodes == null || theDataNodes.length <= 0) {
            throw new IllegalArgumentException("invalid table dataNodes: " + dataNode);
        }

        this.dataNodes = new ArrayList<>(Arrays.asList(theDataNodes));
    }

	/* 设置分表的规则 */
    public void setRule(RuleConfig rule) {
        this.partitionColumn = (rule == null) ? null : rule.getColumn();
        this.partionKeyIsPrimaryKey = (this.partitionColumn == null) ? this.primaryKey == null : this.partitionColumn.equals(this.primaryKey);
        this.rule = rule;
    }

	/* 设置父亲表 */
    public void setParentTC(TableConfig parentTC) {
        this.parentTC = parentTC;
        if (this.parentTC != null) {
            this.locateRTableKeySql = genLocateRootParentSQL();
            this.secondLevel = (parentTC.parentTC == null);
        }
    }

	public TableConfig(){ super(); }
    public TableConfig(
			String name,
			String primaryKey,
			boolean autoIncrement,
			boolean needAddLimit,
            int tableType,
			String dataNode,
			Set<String> dbType,
			RuleConfig rule,
            boolean ruleRequired,
			TableConfig parentTC,
			boolean isChildTable,
			String joinKey,
            String parentKey
	) {
        super();
        this.setName(name);
        this.setDataNode(dataNode);
        this.setRule(rule);
        this.setParentTC(parentTC);
        this.primaryKey = primaryKey;
        this.autoIncrement = autoIncrement;
        this.needAddLimit = needAddLimit;
        this.tableType = tableType;
        this.dbTypes = dbType;
		this.ruleRequired = ruleRequired;
		this.childTable = isChildTable;
		this.joinKey = joinKey;
		this.parentKey = parentKey;
		this.parentTC = parentTC;
        this.checkConfig();
	}

	/* 获得一个sql,感觉和表的外键相关的 TODO */
	public String genLocateRootParentSQL() {
		TableConfig tb = this;
		StringBuilder tableSb = new StringBuilder();
		StringBuilder condition = new StringBuilder();
		TableConfig prevTC = null;
		int level = 0;
		String latestCond = null;
		while (tb.parentTC != null) {
			tableSb.append(tb.parentTC.name).append(',');
			String relation = null;
			if (level == 0) {
				latestCond = " " + tb.parentTC.getName() + '.' + tb.parentKey + "=";
			} else {
				relation = tb.parentTC.getName() + '.' + tb.parentKey + '=' + tb.name + '.' + tb.joinKey;
				condition.append(relation).append(" AND ");
			}
			level++;
			prevTC = tb;
			tb = tb.parentTC;
		}
		// prevTC指向root table
		// tableSb = tableAName,tableBName,rootTableName,
		// condition = talbeBName.parentKey=tableAName.joinKey AND rootTalbeName.parentKey=talbeBName.josinKey AND
		// latestCond = tableBName.parentKey =
		String sql = "SELECT "+prevTC.parentTC.name+'.'+prevTC.parentKey
						+ " FROM " + tableSb.substring(0, tableSb.length() - 1)
						+ " WHERE " + ((level < 2) ? latestCond : condition.toString() + latestCond);
		return sql;
	}


	/* get root parent */
	public TableConfig getRootParent() {
		if (parentTC == null) {
			return null;
		}
		TableConfig preParent = parentTC;
		TableConfig parent = preParent.getParentTC();

		while (parent != null) {
			preParent = parent;
			parent = parent.getParentTC();
		}
		return preParent;
	}

	/* 随机获得一个数据节点 */
	public String getRandomDataNode() {
		int index = Math.abs(rand.nextInt()) % dataNodes.size();
		return dataNodes.get(index);
	}

	/* 检测配置 */
    public void checkConfig(){
        if (this.ruleRequired && this.rule == null) {
            throw new IllegalArgumentException("ruleRequired but rule is null");
        }
    }
}