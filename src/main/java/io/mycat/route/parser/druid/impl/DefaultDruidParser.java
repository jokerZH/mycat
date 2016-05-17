package io.mycat.route.parser.druid.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Condition;
import io.mycat.cache.LayerCachePool;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.DruidParser;
import io.mycat.route.parser.druid.DruidShardingParseInfo;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.RouteCalculateUnit;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.sqlengine.mpp.RangeValue;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/* 对SQLStatement解析 */
public class DefaultDruidParser implements DruidParser {
	protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultDruidParser.class);

	protected DruidShardingParseInfo ctx;	/* 解析得到的结果 */
	private Map<String,String> tableAliasMap = new HashMap<String,String>();	/* 表明映射关系 */
	private List<Condition> conditions = new ArrayList<Condition>();	/* 逻辑表达式的对象 */
	

	/* 使用MycatSchemaStatVisitor解析,得到tables、tableAliasMap、conditions等 */
	public void parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, String originSql,LayerCachePool cachePool,MycatSchemaStatVisitor schemaStatVisitor) throws SQLNonTransientException {
		ctx = new DruidShardingParseInfo();

		//设置为原始sql
		ctx.setSql(originSql);

		//通过visitor解析
		visitorParse(rrs,stmt,schemaStatVisitor);

		//通过Statement解析
		statementParse(schema, rrs, stmt);
		
		//改写sql：如insert语句主键自增长的可以
		changeSql(schema, rrs, stmt,cachePool);
	}
	
	/* 通过satement的方式获得sql信息,子类通过覆盖使用 */
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException { }
	
	@Override
	/* 改写sql */
	public void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,LayerCachePool cachePool) throws SQLNonTransientException { }

	@Override
	/* 通过visitor获得sql中的信息 */
	public void visitorParse(RouteResultset rrs, SQLStatement stmt,MycatSchemaStatVisitor visitor) throws SQLNonTransientException{
		stmt.accept(visitor);
		
		List<List<Condition>> mergedConditionList = new ArrayList<List<Condition>>();
		if(visitor.hasOrCondition()) {
			// 包含or语句 根据or拆分
			mergedConditionList = visitor.splitConditions();
		} else {
			//不包含OR语句
			mergedConditionList.add(visitor.getConditions());
		}
		
		if(visitor.getAliasMap() != null) {
			// 有别名存在
			for(Map.Entry<String, String> entry : visitor.getAliasMap().entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();
				if(key != null && key.indexOf("`") >= 0) { key = key.replaceAll("`", ""); }
				if(value != null && value.indexOf("`") >= 0) { value = value.replaceAll("`", ""); }

				// 表名前面带database的，去掉
				if(key != null) {
					int pos = key.indexOf(".");
					if(pos> 0) {
						key = key.substring(pos + 1);
					}
				}
				
				if(key.equals(value)) {
					ctx.addTable(key.toUpperCase());
				} else {
					tableAliasMap.put(key, value);
				}
			}
			ctx.setTableAliasMap(tableAliasMap);
		}

		ctx.setRouteCalculateUnits(this.buildRouteCalculateUnits(visitor, mergedConditionList));
	}

	/* 将sql语句中各个表达式转化成各个字段的范围 */
	private List<RouteCalculateUnit> buildRouteCalculateUnits(SchemaStatVisitor visitor, List<List<Condition>> conditionList) {
		List<RouteCalculateUnit> retList = new ArrayList<RouteCalculateUnit>();

		// 遍历condition ，找分片字段
		for(int i = 0; i < conditionList.size(); i++) {
			RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
			for(Condition condition : conditionList.get(i)) {
				List<Object> values = condition.getValues();
				if(values.size() == 0) {
					break;
				}
				if(checkConditionValues(values)) {
					String columnName = StringUtil.removeBackquote(condition.getColumn().getName().toUpperCase());
					String tableName = StringUtil.removeBackquote(condition.getColumn().getTable().toUpperCase());
					if(visitor.getAliasMap() != null && visitor.getAliasMap().get(condition.getColumn().getTable()) == null) {
						// 子查询的别名条件忽略掉,不参数路由计算，否则后面找不到表
						continue;
					}
					
					String operator = condition.getOperator();
					
					//只处理between ,in和=
					if(operator.equals("between")) {
						RangeValue rv = new RangeValue(values.get(0), values.get(1), RangeValue.EE);
						routeCalculateUnit.addShardingExpr(tableName.toUpperCase(), columnName, rv);

					} else if(operator.equals("=") || operator.toLowerCase().equals("in")){
						//只处理=号和in操作符,其他忽略
						routeCalculateUnit.addShardingExpr(tableName.toUpperCase(), columnName, values.toArray());
					}
				}
			}
			retList.add(routeCalculateUnit);
		}
		return retList;
	}

	/* 检查values中是否存在null或者空值 */
	private boolean checkConditionValues(List<Object> values) {
		for(Object value : values) {
			if(value != null && !value.toString().equals("")) {
				return true;
			}
		}
		return false;
	}

	public Map<String, String> getTableAliasMap() { return tableAliasMap; }
	public List<Condition> getConditions() { return conditions; }
	public DruidShardingParseInfo getCtx() { return ctx; }
}
