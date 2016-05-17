package io.mycat.route.parser.druid;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/* 解析结果 */
public class DruidShardingParseInfo {
	private List<WhereUnit> whereUnits = new ArrayList<WhereUnit>(); 	/* 一个sql中可能有多个WhereUnit（如子查询中的where可能导致多个）*/
	private List<RouteCalculateUnit> routeCalculateUnits = new ArrayList<RouteCalculateUnit>();	/* TODO */
	
	private String sql = "";	/* sql语句 */
	private List<String> tables = new ArrayList<String>();	/* 逻辑表名 */
	private Map<String/*别名*/, String/*真名*/> tableAliasMap = new LinkedHashMap<String, String>();	/* 表名的映射关系 */

	public Map<String, String> getTableAliasMap() { return tableAliasMap; }
	public void setTableAliasMap(Map<String, String> tableAliasMap) { this.tableAliasMap = tableAliasMap; }
	public String getSql() { return sql; }
	public void setSql(String sql) { this.sql = sql; }
	public List<String> getTables() { return tables; }
	public void addTable(String tableName) { this.tables.add(tableName); }
	public RouteCalculateUnit getRouteCalculateUnit() { return routeCalculateUnits.get(0); }
	public List<RouteCalculateUnit> getRouteCalculateUnits() { return routeCalculateUnits; }
	public void setRouteCalculateUnits(List<RouteCalculateUnit> routeCalculateUnits) { this.routeCalculateUnits = routeCalculateUnits; }
	public void addRouteCalculateUnit(RouteCalculateUnit routeCalculateUnit) { this.routeCalculateUnits.add(routeCalculateUnit); }

	public void clear() {
		for(RouteCalculateUnit unit : routeCalculateUnits ) { unit.clear(); }
	}
}
