package io.mycat.route;

import io.mycat.cache.LayerCachePool;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SystemConfig;

import java.sql.SQLNonTransientException;

/* 路由策略接口 */
public interface RouteStrategy {
	public RouteResultset route(
			SystemConfig sysConfig,		/* 系统配置 */
			SchemaConfig schema,		/* 逻辑DB */
			int sqlType,				/* sql语句的类型 */
			String origSQL,				/* sql语句 */
			String charset,				/* 字符集 */
			MySQLFrontConnection sc,	/* 客户端连接 */
			LayerCachePool cachePool	/* 结果缓存 */
	) throws SQLNonTransientException;
}
