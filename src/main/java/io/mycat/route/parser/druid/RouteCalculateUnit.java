package io.mycat.route.parser.druid;

import io.mycat.sqlengine.mpp.ColumnRoutePair;
import io.mycat.sqlengine.mpp.RangeValue;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/* 路由计算单元 */
public class RouteCalculateUnit {
	private Map<String/*tableName*/, Map<String/*columnName*/, Set<ColumnRoutePair>>> tablesAndConditions = new LinkedHashMap<String, Map<String, Set<ColumnRoutePair>>>();	/* TODO */

	/* 这里根据value的类型来决定操作类型,要么等于,要么范围 */
	public void addShardingExpr(String tableName/*逻辑表名*/, String columnName/*字段名*/, Object value/*值*/) {
		Map<String, Set<ColumnRoutePair>> tableColumnsMap = tablesAndConditions.get(tableName);
		
		if (value == null) { return; }
		
		if (tableColumnsMap == null) {
			tableColumnsMap = new LinkedHashMap<String, Set<ColumnRoutePair>>();
			tablesAndConditions.put(tableName, tableColumnsMap);
		}
		
		String uperColName = columnName.toUpperCase();
		Set<ColumnRoutePair> columValues = tableColumnsMap.get(uperColName);

		if (columValues == null) {
			columValues = new LinkedHashSet<ColumnRoutePair>();
			tablesAndConditions.get(tableName).put(uperColName, columValues);
		}

		if (value instanceof Object[]) {
			for (Object item : (Object[]) value) {
				if(item == null) {
					continue;
				}
				columValues.add(new ColumnRoutePair(item.toString()));
			}
		} else if (value instanceof RangeValue) {
			columValues.add(new ColumnRoutePair((RangeValue) value));
		} else {
			columValues.add(new ColumnRoutePair(value.toString()));
		}
	}
	
	public void clear() { tablesAndConditions.clear(); }
	public Map<String, Map<String, Set<ColumnRoutePair>>> getTablesAndConditions() { return tablesAndConditions; }
}
