package io.mycat.route.parser.druid;

import io.mycat.cache.LayerCachePool;
import io.mycat.route.RouteResultset;
import io.mycat.server.config.node.SchemaConfig;

import java.sql.SQLNonTransientException;

import com.alibaba.druid.sql.ast.SQLStatement;

/* 对SQLStatement解析 */
/* 主要通过visitor解析和statement解析：有些类型的SQLStatement通过visitor解析足够 */
public interface DruidParser {
	/* 使用MycatSchemaStatVisitor解析,得到tables、tableAliasMap、conditions等 */
	public void parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, String originSql,LayerCachePool cachePool,MycatSchemaStatVisitor schemaStatVisitor) throws SQLNonTransientException;
	
	/* statement方式解析 如果visitorParse解析得不到表名、字段等信息的，就通过覆盖该方法来解析 */
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException;

	/* 通过visitor解析：有些类型的Statement通过visitor解析得不到表名 */
	public void visitorParse(RouteResultset rrs, SQLStatement stmt,MycatSchemaStatVisitor visitor) throws SQLNonTransientException;
	
	/* 改写sql：加limit，加group by、加order by如有些没有加limit的可以通过该方法增加 */
	public void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,LayerCachePool cachePool) throws SQLNonTransientException;

	/* 获取解析到的信息 */
	public DruidShardingParseInfo getCtx();
}
