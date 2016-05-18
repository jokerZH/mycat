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

import io.mycat.sqlengine.mpp.HavingCols;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/* seelct sql语句负责处理相关 */
public class SQLMerge implements Serializable {
	private LinkedHashMap<String, Integer> orderByCols;	/* order的字段名 */
	private HavingCols havingCols;						/* having中的表达式 如A>0 */
	private Map<String/*聚合函数名*/, Integer/*类型*/> mergeCols;	/* 聚合函数列表 */
	private String[] groupByCols;						/* group by 的字段名 */
	private boolean hasAggrColumn;						/* 是否有聚合函数 如count sum等 */

	public LinkedHashMap<String, Integer> getOrderByCols() { return orderByCols; }
	public void setOrderByCols(LinkedHashMap<String, Integer> orderByCols) { this.orderByCols = orderByCols; }
	public Map<String, Integer> getMergeCols() { return mergeCols; }
	public void setMergeCols(Map<String, Integer> mergeCols) { this.mergeCols = mergeCols; }
	public String[] getGroupByCols() { return groupByCols; }
	public void setGroupByCols(String[] groupByCols) { this.groupByCols = groupByCols; }
	public boolean isHasAggrColumn() { return hasAggrColumn; }
	public void setHasAggrColumn(boolean hasAggrColumn) { this.hasAggrColumn = hasAggrColumn; }
	public HavingCols getHavingCols() { return havingCols; }
	public void setHavingCols(HavingCols havingCols) { this.havingCols = havingCols; }
}