package io.mycat.route.handler;

import io.mycat.cache.LayerCachePool;
import io.mycat.route.RouteResultset;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SystemConfig;
import java.sql.SQLNonTransientException;

/* 按照注释中包含指定类型的内容做路由解析 */
public interface HintHandler {

	/* hint路由操作 */
	public RouteResultset route(
			SystemConfig sysConfig,		/* 系统配置 */
			SchemaConfig schema,		/* 逻辑db */
			int sqlType, 				/* sql类型 */
			String realSQL, 			/* 真实的sql */
			String charset,				/* 字符集 */
			MySQLFrontConnection sc,	/* 客户端连接 */
			LayerCachePool cachePool,	/* 二级缓存 */
			String hintSQLValue			/* hint中包含的值 */
	) throws SQLNonTransientException;
}
