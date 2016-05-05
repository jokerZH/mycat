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


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/* 逻辑db的配置信息 */
public class SchemaConfig {
    private Random random = new Random();

    private String name;						/* ldbName */
    private Map<String, TableConfig> tables; 	/* 逻辑db上的表 */
    private boolean noSharding;					/* 是否是分库 */
    private String dataNode;					/* 默认后端DB */
    private Set<String/*sliceName*/> metaDataNodes;			/* 存放db信息,用于查看逻辑表信息的时候使用,所以一个分表逻辑表只需要保存一个db就好了 */
    private Set<String/*sliceName*/> allDataNodes;			/* 存放所有db信息 */

    private int defaultMaxLimit;		/* 当sql没有limit限制的时候,会自动加上defaultMaxLimit的限制 */
    private boolean checkSQLSchema;		/* 不支持 dbName.tableName的模式 */
    private Map<String/*join relation ,A.ID=B.PARENT_ID*/, TableConfig/*rootTable*/> joinRel2TableMap = new HashMap<String, TableConfig>(); /* TODO 有join关系的表 if a->b*->c* ,then A is root table */
    private String[] allDataNodeStrArr;					/* allDataNodex的数组形式 */
    private boolean needSupportMultiDBType = false;		/* TODO */
    private String defaultDataNodeDbType;				/* TODO */
    private Map<String/*sliceName*/, String/*类型*/> dataNodeDbTypeMap = new HashMap<>();	/* slice和后端类型的映射关系 */

	public SchemaConfig(){ super(); }
	public SchemaConfig(
			String name,
			String dataNode,
			Map<String, TableConfig> tables,
			int defaultMaxLimit,
			boolean checkSQLschema
	) {
		this.name = name;
		this.dataNode = dataNode;
		this.checkSQLSchema = checkSQLschema;
		this.defaultMaxLimit = defaultMaxLimit;
        this.setTables(tables);
	}

    public void setTables(Map<String, TableConfig> tables) {
        this.tables = tables;
        buildJoinMap(tables);

        this.noSharding = (tables == null || tables.isEmpty());
        if (noSharding && dataNode == null) {
            throw new RuntimeException(name + " in noSharding mode schema must have default dataNode ");
        }

        this.metaDataNodes = buildMetaDataNodes();
        this.allDataNodes = buildAllDataNodes();

        if (this.allDataNodes != null && !this.allDataNodes.isEmpty()) {
            String[] dnArr = new String[this.allDataNodes.size()];
            dnArr = this.allDataNodes.toArray(dnArr);
            this.allDataNodeStrArr = dnArr;
        } else {
            this.allDataNodeStrArr = null;
        }
    }

	public String getDefaultDataNodeDbType() { return defaultDataNodeDbType; }
	public void setDefaultDataNodeDbType(String defaultDataNodeDbType) { this.defaultDataNodeDbType = defaultDataNodeDbType; }
	public boolean isCheckSQLSchema() { return checkSQLSchema; }
	public void setName(String name) { this.name = name; }
	public int getDefaultMaxLimit() { return defaultMaxLimit; }
	public void setCheckSQLSchema(boolean checkSQLSchema) { this.checkSQLSchema = checkSQLSchema; }
	public void setDefaultMaxLimit(int defaultMaxLimit) { this.defaultMaxLimit = defaultMaxLimit; }
	public boolean isNeedSupportMultiDBType()  { return needSupportMultiDBType; }
	public void setNeedSupportMultiDBType(boolean needSupportMultiDBType)  { this.needSupportMultiDBType = needSupportMultiDBType; }
	public Map<String, TableConfig> getJoinRel2TableMap() { return joinRel2TableMap; }
	public String getName() { return name; }
	public String getDataNode() {return dataNode; }
	public void setDataNode(String dataNode) { this.dataNode = dataNode; }
	public Map<String, TableConfig> getTables() { return tables; }
	public boolean isNoSharding() { return noSharding; }
	public Set<String> getMetaDataNodes() { return metaDataNodes; }
	public Set<String> getAllDataNodes() { return allDataNodes; }
	public Map<String, String> getDataNodeDbTypeMap()  { return dataNodeDbTypeMap; }
	public void setDataNodeDbTypeMap(Map<String, String> dataNodeDbTypeMap)  { this.dataNodeDbTypeMap = dataNodeDbTypeMap; }


	/* 构建join表的关系, 结果子啊joinRel2TalbeMap中 */
	private void buildJoinMap(Map<String, TableConfig> tables2) {
		if (tables == null || tables.isEmpty()) {
			return;
		}

		for (TableConfig tc : tables.values()) {
			if (tc.isChildTable()) {
				TableConfig rootTc = tc.getRootParent();
				/* 最多两层 FIXME */
				String joinRel1 = tc.getName() + '.' + tc.getJoinKey() + '=' + tc.getParentTC().getName() + '.' + tc.getParentKey();
				String joinRel2 = tc.getParentTC().getName() + '.' + tc.getParentKey() + '=' + tc.getName() + '.' + tc.getJoinKey();
				joinRel2TableMap.put(joinRel1, rootTc);
				joinRel2TableMap.put(joinRel2, rootTc);
			}
		}
	}

	/* 随机获得一个db */
	public String getRandomDataNode() {
		if (this.allDataNodeStrArr == null) {
			return null;
		}
		int index = Math.abs(random.nextInt()) % allDataNodeStrArr.length;
		return this.allDataNodeStrArr[index];
	}

	/* 取得含有不同Meta信息的数据节点,比如表和表结构。 */
	private Set<String> buildMetaDataNodes() {
		Set<String> set = new HashSet<String>();
		if (!isEmpty(dataNode)) {
			set.add(dataNode);
		}
		if (!noSharding) {
			for (TableConfig tc : tables.values()) {
				set.add(tc.getDataNodes().get(0));
			}
		}

		return set;
	}

	/**
	 * 取得该schema的所有数据节点
	 */
	private Set<String> buildAllDataNodes() {
		Set<String> set = new HashSet<String>();
		if (!isEmpty(dataNode)) {
			set.add(dataNode);
		}
		if (!noSharding) {
			for (TableConfig tc : tables.values()) {
				set.addAll(tc.getDataNodes());
			}
		}
		return set;
	}

	private static boolean isEmpty(String str) { return ((str == null) || (str.length() == 0)); }
}