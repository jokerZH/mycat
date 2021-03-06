package io.mycat.route.impl;

import io.mycat.MycatServer;
import io.mycat.cache.LayerCachePool;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteStrategy;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SystemConfig;
import io.mycat.server.parser.ServerParse;
import io.mycat.sqlengine.mpp.LoadData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;

/* 路由过程的框架 */
public abstract class AbstractRouteStrategy implements RouteStrategy {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRouteStrategy.class);

	@Override
	public RouteResultset route(
			SystemConfig sysConfig,
			SchemaConfig schema,
			int sqlType,
			String origSQL,
			String charset,
			MySQLFrontConnection sc,
			LayerCachePool cachePool
	) throws SQLNonTransientException
	{
		// 在route前判断特殊情况, 走特殊逻辑
		if (beforeRouteProcess(schema, sqlType, origSQL, sc)) return null;

		// 有机会修改sql
		String stmt = MycatServer.getInstance().getSqlInterceptor().interceptSQL(origSQL, sqlType);

		if (origSQL != stmt && LOGGER.isDebugEnabled()) {
			LOGGER.debug("sql intercepted to " + stmt + " from " + origSQL);
		}

		if (schema.isCheckSQLSchema()) {
			stmt = RouterUtil.removeSchema(stmt, schema.getName());
		}

		RouteResultset rrs = new RouteResultset(stmt, sqlType);

        if ( LOGGER.isDebugEnabled()&&origSQL.startsWith(LoadData.loadDataHint)) {
			//优化debug loaddata输出cache的日志会极大降低性能
			rrs.setCacheAble(false);
		}

		// 设置是否自动提交
		if (sc != null ) {
			rrs.setAutocommit(sc.isAutocommit());
		}

		// 处理ddl的命令 需要所有都要处理下
		if(ServerParse.DDL==sqlType){
			return RouterUtil.routeToDDLNode(rrs, sqlType, stmt,schema);
		}

		if (schema.isNoSharding() && ServerParse.SHOW != sqlType) {
			// 不是sharding模式
			rrs = RouterUtil.routeToSingleNode(rrs, schema.getDataNode(), stmt);
		} else {
			// sharding模式
			// 是否是一些查询系统数据的语句，那样的话，发送给某些slice就好了
			RouteResultset returnedSet=routeSystemInfo(schema, sqlType, stmt, rrs);
			if(returnedSet==null){
				// 处理一般流程
				rrs = routeNormalSqlWithAST(schema, stmt, rrs, charset, cachePool);
			}
		}

		return rrs;
	}

	/* 判断一些特殊情况,如果是,则直接执行特殊路径 */
	private boolean beforeRouteProcess(SchemaConfig schema, int sqlType, String origSQL, MySQLFrontConnection sc) throws SQLNonTransientException {
		return 	(RouterUtil.processWithMycatSeq(schema, sqlType, origSQL, sc)) ||
                (sqlType == ServerParse.INSERT && RouterUtil.processERChildTable(schema, origSQL, sc)) ||
				(sqlType == ServerParse.INSERT && RouterUtil.processInsert(schema, sqlType, origSQL,sc));
	}

	/* 通过解析AST语法树类来寻找路由 */
	public abstract RouteResultset routeNormalSqlWithAST(SchemaConfig schema,String stmt,RouteResultset rrs,String charset,LayerCachePool cachePool) throws SQLNonTransientException;

	/* TODO  */
	public abstract RouteResultset routeSystemInfo(SchemaConfig schema,int sqlType,String stmt,RouteResultset rrs) throws SQLSyntaxErrorException;

	/* show语句处理 */
	public abstract RouteResultset analyseShowSQL(SchemaConfig schema,RouteResultset rrs, String stmt) throws SQLNonTransientException;
}
