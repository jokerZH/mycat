package io.mycat.route.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.base.Strings;
import io.mycat.cache.LayerCachePool;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.parser.druid.*;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/* 负责路由功能 默认的路由器 */
public class DruidMycatRouteStrategy extends AbstractRouteStrategy {
	private static final Logger LOGGER = LoggerFactory.getLogger(DruidMycatRouteStrategy.class);

	@Override
	/* 一般sql的执行过程 */
	public RouteResultset routeNormalSqlWithAST(SchemaConfig schema, String stmt, RouteResultset rrs, String charset, LayerCachePool cachePool) throws SQLNonTransientException {
		SQLStatementParser parser =null;
		if(schema.isNeedSupportMultiDBType()) {
			parser = new MycatStatementParser(stmt);
		}else {
			//只有mysql时只支持mysql语法
			parser = new MySqlStatementParser(stmt);
		}

		MycatSchemaStatVisitor visitor = null;
		SQLStatement statement;
		try {
			// 解析sql语句
			statement = parser.parseStatement();
            visitor = new MycatSchemaStatVisitor();
		} catch (Exception t) {
			//解析出现问题统一抛SQL语法错误
	        LOGGER.error("DruidMycatRouteStrategyError", t);
			throw new SQLSyntaxErrorException(t);
		}

		// 检验unsupported statement
		checkUnSupportedStatement(statement);

		// 构建解析器
        DruidParser druidParser = DruidParserFactory.create(schema,statement,visitor);
		druidParser.parser(schema, rrs, statement, stmt,cachePool,visitor);

		//DruidParser解析过程中已完成了路由的直接返回
		if(rrs.isFinishedRoute()) {
			return rrs;
		}
		
//		rrs.setStatement(druidParser.getCtx().getSql());
		//没有from的的select语句或其他
		if(druidParser.getCtx().getTables().size() == 0) {
			return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(),druidParser.getCtx().getSql());
		}

		if(druidParser.getCtx().getRouteCalculateUnits().size() == 0) {
			RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
			druidParser.getCtx().addRouteCalculateUnit(routeCalculateUnit);
		}
		
		SortedSet<RouteResultsetNode> nodeSet = new TreeSet<RouteResultsetNode>();
		for(RouteCalculateUnit unit : druidParser.getCtx().getRouteCalculateUnits()) {
			RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, druidParser.getCtx(), unit, rrs, isSelect(statement), cachePool);
			if(rrsTmp != null) {
				for(RouteResultsetNode node :rrsTmp.getNodes()) {
					nodeSet.add(node);
				}
			}
		}
		
		RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSet.size()];
		int i = 0;
		for (Iterator iterator = nodeSet.iterator(); iterator.hasNext();) {
			nodes[i] = (RouteResultsetNode) iterator.next();
			i++;
			
		}
		
		rrs.setNodes(nodes);
		

		return rrs;
	}



    private boolean isSelect(SQLStatement statement) {
		if(statement instanceof SQLSelectStatement) {
			return true;
		}
		return false;
	}

	/* sql语句是获得系统信息类的sql语句 */
	@Override
	public RouteResultset routeSystemInfo(SchemaConfig schema, int sqlType, String stmt, RouteResultset rrs) throws SQLSyntaxErrorException {
		switch(sqlType) {
			case ServerParse.SHOW:
				// Show sql语句
				return analyseShowSQL(schema, rrs, stmt);

			case ServerParse.SELECT:
				//select @@类的语句
				if (stmt.contains("@@")) {
					return analyseDoubleAtSgin(schema, rrs, stmt);
				}
				break;

			case ServerParse.DESCRIBE:
				// describe table
				int ind = stmt.indexOf(' ');
				stmt = stmt.trim();
				return analyseDescrSQL(schema, rrs, stmt, ind + 1);
		}
		return null;
	}

	/* show 类命令的sql语句处理方法 */
	@Override
	public RouteResultset analyseShowSQL(SchemaConfig schema, RouteResultset rrs, String stmt) throws SQLSyntaxErrorException {
		String upStmt = stmt.toUpperCase();
		int tabInd = upStmt.indexOf(" TABLES");
		if (tabInd > 0) {
			// show tables
			int[] nextPost = RouterUtil.getSpecPos(upStmt, 0);
			if (nextPost[0] > 0) {
				// remove db info
				int end = RouterUtil.getSpecEndPos(upStmt, tabInd);
				if (upStmt.indexOf(" FULL") > 0) {
					stmt = "SHOW FULL TABLES" + stmt.substring(end);
				} else {
					stmt = "SHOW TABLES" + stmt.substring(end);
				}
			}
            String defaultNode=  schema.getDataNode();
            if(!Strings.isNullOrEmpty(defaultNode)) {
				// 发送给默认slice
				return RouterUtil.routeToSingleNode(rrs, defaultNode, stmt);
			}

			// 给所有涉及到的db发送请求
			return RouterUtil.routeToMultiNode(false, rrs, schema.getMetaDataNodes(), stmt);
		}

		// show index or column
		int[] indx = RouterUtil.getSpecPos(upStmt, 0);
		if (indx[0] > 0) {
			// has table
			int[] repPos = { indx[0] + indx[1], 0 };
			String tableName = RouterUtil.getShowTableName(stmt, repPos);
			// IN DB pattern
			int[] indx2 = RouterUtil.getSpecPos(upStmt, indx[0] + indx[1] + 1);
			if (indx2[0] > 0) {
				// find LIKE OR WHERE
				repPos[1] = RouterUtil.getSpecEndPos(upStmt, indx2[0] + indx2[1]);
			}
			stmt = stmt.substring(0, indx[0]) + " FROM " + tableName + stmt.substring(repPos[1]);

			/* 找到逻辑表的一个slice执行 */
			RouterUtil.routeForTableMeta(rrs, schema, tableName, stmt);
			return rrs;
		}

		// show create table tableName
		int[] createTabInd = RouterUtil.getCreateTablePos(upStmt, 0);
		if (createTabInd[0] > 0) {
			int tableNameIndex = createTabInd[0] + createTabInd[1];
			if (upStmt.length() > tableNameIndex) {
				String tableName = stmt.substring(tableNameIndex).trim();
				int ind2 = tableName.indexOf('.');
				if (ind2 > 0) {
					tableName = tableName.substring(ind2 + 1);
				}

				/* 找到逻辑表的一个slice执行 */
				RouterUtil.routeForTableMeta(rrs, schema, tableName, stmt);
				return rrs;
			}
		}

		/* 随机发送给逻辑DB的一个slice */
		return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), stmt);
	}

	/* 对Desc语句进行分析 返回数据路由集合 */
	private static RouteResultset analyseDescrSQL(SchemaConfig schema, RouteResultset rrs, String stmt, int ind/*第一个' '的位置*/) {
		final String MATCHED_FEATURE = "DESCRIBE ";
		final String MATCHED2_FEATURE = "DESC ";
		int pos = 0;
		while (pos < stmt.length()) {
			char ch = stmt.charAt(pos);
			// 忽略处理注释 /* */ BEN
			if(ch == '/' &&  pos+4 < stmt.length() && stmt.charAt(pos+1) == '*') {
				if(stmt.substring(pos+2).indexOf("*/") != -1) {
					pos += stmt.substring(pos+2).indexOf("*/")+4;
					continue;
				} else {
					// 不应该发生这类情况。
					throw new IllegalArgumentException("sql 注释 语法错误");
				}
			} else if(ch == 'D'||ch == 'd') {
				// 匹配 [describe ] 
				if(pos+MATCHED_FEATURE.length() < stmt.length() && (stmt.substring(pos).toUpperCase().indexOf(MATCHED_FEATURE) != -1)) {
					pos = pos + MATCHED_FEATURE.length();
					break;
				} else if(pos+MATCHED2_FEATURE.length() < stmt.length() && (stmt.substring(pos).toUpperCase().indexOf(MATCHED2_FEATURE) != -1)) {
					pos = pos + MATCHED2_FEATURE.length();
					break;
				} else {
					pos++;
				}
			}
		}
		
		// 重置ind坐标。BEN GONG
		ind = pos;
		
		int[] repPos = { ind, 0 };
		String tableName = RouterUtil.getTableName(stmt, repPos);
		
		stmt = stmt.substring(0, ind) + tableName + stmt.substring(repPos[1]);
		RouterUtil.routeForTableMeta(rrs, schema, tableName, stmt);
		return rrs;
	}
	
	/* 处理 select @@ 类数据 */
	private RouteResultset analyseDoubleAtSgin(SchemaConfig schema, RouteResultset rrs, String stmt) throws SQLSyntaxErrorException {
		String upStmt = stmt.toUpperCase();

		int atSginInd = upStmt.indexOf(" @@");
		if (atSginInd > 0) {
			return RouterUtil.routeToMultiNode(false, rrs, schema.getMetaDataNodes(), stmt);
		}

		return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), stmt);
	}

	/* 检验不支持的SQLStatement类型 */
	private void checkUnSupportedStatement(SQLStatement statement) throws SQLSyntaxErrorException {
		if(statement instanceof MySqlReplaceStatement) {
			//不支持replace语句
			throw new SQLSyntaxErrorException(" ReplaceStatement can't be supported,use insert into ...on duplicate key update... instead ");
		}
	}
}