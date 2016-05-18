package io.mycat.route.parser.druid.impl;

import io.mycat.MycatServer;
import io.mycat.cache.LayerCachePool;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.parser.druid.RouteCalculateUnit;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ErrorCode;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.TableConfig;
import io.mycat.sqlengine.mpp.ColumnRoutePair;
import io.mycat.sqlengine.mpp.HavingCols;
import io.mycat.sqlengine.mpp.MergeCol;
import io.mycat.sqlengine.mpp.OrderCol;
import io.mycat.util.ObjectUtil;
import io.mycat.util.StringUtil;

import java.sql.SQLNonTransientException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlSelectGroupByExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock.Limit;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnionQuery;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.druid.wall.spi.WallVisitorUtils;

/* mysql的select */
public class DruidSelectParser extends DefaultDruidParser {
    protected boolean isNeedParseOrderAgg=true;

    @Override
    /* 提取sql中的信息 结果存放在rrs中 */
    public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) {
        SQLSelectStatement selectStmt = (SQLSelectStatement)stmt;
        SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();

        if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            // 单个select语句
            MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();

            // 获得sql中的信息,并对avg函数修改sql语句
            parseOrderAggGroupMysql(schema, stmt,rrs, mysqlSelectQuery);

            // 更改canRunInReadDB属性
            if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode()) && rrs.isAutocommit() == false) {
                rrs.setCanRunInReadDB(false);
            }

        } else if (sqlSelectQuery instanceof MySqlUnionQuery) {
            // 多个select语句联合 FIXME
        }
    }

    @Override
    /* 计算结果slice 并根据结果改写sql：需要加limit的加上 */
    public void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,LayerCachePool cachePool) throws SQLNonTransientException {
        // 获得计算表达式对应的slice,结果存在rrs中
        tryRoute(schema, rrs, cachePool);

        // 增加limit
        rrs.copyLimitToNodes();

        SQLSelectStatement selectStmt = (SQLSelectStatement)stmt;
        SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
        if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            // 单个select语句
            MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();

            // 清楚groupby中的having  TODO why?
            SQLSelectGroupByClause groupByClause = mysqlSelectQuery.getGroupBy();
            if(groupByClause != null && groupByClause.getHaving() != null){
                groupByClause.setHaving(null);
            }

            // 增加limit
            int limitStart = 0;
            int limitSize = schema.getDefaultMaxLimit();
            Map<String, Map<String, Set<ColumnRoutePair>>> allConditions = getAllConditions();
            boolean isNeedAddLimit = isNeedAddLimit(schema, rrs, mysqlSelectQuery, allConditions);
            if(isNeedAddLimit) {
                Limit limit = new Limit();
                limit.setRowCount(new SQLIntegerExpr(limitSize));
                mysqlSelectQuery.setLimit(limit);
                rrs.setLimitSize(limitSize);
                String sql= getSql(rrs, stmt, isNeedAddLimit);
                rrs.changeNodeSqlAfterAddLimit(schema, getCurentDbType(), sql, 0, limitSize, true);
            }

            // 将sql中的limit信息存放到rss中
            Limit limit = mysqlSelectQuery.getLimit();
            if(limit != null&&!isNeedAddLimit) {
                SQLIntegerExpr offset = (SQLIntegerExpr)limit.getOffset();
                SQLIntegerExpr count = (SQLIntegerExpr)limit.getRowCount();
                if(offset != null) {
                    limitStart = offset.getNumber().intValue();
                    rrs.setLimitStart(limitStart);
                }
                if(count != null) {
                    limitSize = count.getNumber().intValue();
                    rrs.setLimitSize(limitSize);
                }

                // 分表下 limit如何实现
                if(isNeedChangeLimit(rrs)) {
                    Limit changedLimit = new Limit();
                    // FIXME 特么start大怎么办,不用脑子想想么
                    changedLimit.setRowCount(new SQLIntegerExpr(limitStart + limitSize));

                    if(offset != null) {
                        if(limitStart < 0) {
                            String msg = "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '" + limitStart + "'";
                            throw new SQLNonTransientException(ErrorCode.ER_PARSE_ERROR + " - " + msg);
                        } else {
                            changedLimit.setOffset(new SQLIntegerExpr(0));
                        }
                    }

                    mysqlSelectQuery.setLimit(changedLimit);

                    String sql= getSql(rrs, stmt, isNeedAddLimit);
                    rrs.changeNodeSqlAfterAddLimit(schema,getCurentDbType(),sql,0, limitStart + limitSize, true);

                    //设置改写后的sql
                    ctx.setSql(sql);

                } else {
                    rrs.changeNodeSqlAfterAddLimit(schema,getCurentDbType(),getCtx().getSql(),rrs.getLimitStart(), rrs.getLimitSize(), true);
                }
            }

            rrs.setCacheAble(isNeedCache(schema, rrs, mysqlSelectQuery, allConditions));
        }
    }



    /* 提起select语句中的信息,同时对于一些聚合函数,修改sql语句 */
    protected void parseOrderAggGroupMysql(SchemaConfig schema, SQLStatement stmt, RouteResultset rrs, MySqlSelectQueryBlock mysqlSelectQuery)
    {
        if(!isNeedParseOrderAgg)  { return; }
        Map<String, String> aliaColumns = parseAggGroupCommon(schema, stmt, rrs, mysqlSelectQuery);

        // 活的OrderBy中的字段值
        if(mysqlSelectQuery.getOrderBy() != null) {
            List<SQLSelectOrderByItem> orderByItems = mysqlSelectQuery.getOrderBy().getItems();
            rrs.setOrderByCols(buildOrderByCols(orderByItems,aliaColumns));
        }
        isNeedParseOrderAgg=false;
    }

    /* 解析聚合函数, Distinct, groupBy的情况,并根据聚合函数在分表情况的策略修改sql*/
    protected Map<String/*字段名*/, String/*字段别名*/> parseAggGroupCommon(SchemaConfig schema, SQLStatement stmt, RouteResultset rrs, SQLSelectQueryBlock mysqlSelectQuery)
    {
        Map<String, String> aliaColumns = new HashMap<String, String>();    /* 字段名 假名和真名的映射关系 */
        Map<String/*聚合函数名*/, Integer/*聚合函数类型*/> aggrColumns = new HashMap<String, Integer>();

        boolean isNeedChangeSql=false;

        // 获得字段名列表
        List<SQLSelectItem> selectList = mysqlSelectQuery.getSelectList();
        int size = selectList.size();
        for (int i = 0; i < size; i++)
        {
            SQLSelectItem item = selectList.get(i);

            if (item.getExpr() instanceof SQLAggregateExpr) {
                // 聚合函数 如sum min max等
                SQLAggregateExpr expr = (SQLAggregateExpr) item.getExpr();
                String method = expr.getMethodName();

                //只处理有别名的情况，无别名添加别名，否则某些数据库会得不到正确结果处理
                int mergeType = MergeCol.getMergeType(method);
                if (MergeCol.MERGE_AVG == mergeType&&isRoutMultiNode(schema,rrs)) {
                    //跨分片avg需要特殊处理，直接avg结果是不对的 增加sum和Count,然后在本地计算avg

                    String colName = item.getAlias() != null ? item.getAlias() : method + i;

                    // add sum
                    SQLSelectItem sum =new SQLSelectItem();
                    String sumColName = colName + "SUM";
                    sum.setAlias(sumColName);
                    SQLAggregateExpr sumExp =new SQLAggregateExpr("SUM");
                    ObjectUtil.copyProperties(expr, sumExp);
                    sumExp.getArguments().addAll(expr.getArguments());
                    sumExp.setMethodName("SUM");
                    sum.setExpr(sumExp);
                    selectList.set(i, sum);
                    aggrColumns.put(sumColName, MergeCol.MERGE_SUM);

                    // add count
                    SQLSelectItem count =new SQLSelectItem();
                    String countColName = colName + "COUNT";
                    count.setAlias(countColName);
                    SQLAggregateExpr countExp = new SQLAggregateExpr("COUNT");
                    ObjectUtil.copyProperties(expr,countExp);
                    countExp.getArguments().addAll(expr.getArguments());
                    countExp.setMethodName("COUNT");
                    count.setExpr(countExp);
                    selectList.add(count);
                    aggrColumns.put(countColName, MergeCol.MERGE_COUNT);

                    isNeedChangeSql=true;
                    aggrColumns.put(colName, mergeType);
                    rrs.setHasAggrColumn(true);

                } else if (MergeCol.MERGE_UNSUPPORT != mergeType) {
                    // 非avg的聚合函数
                    if (item.getAlias() != null && item.getAlias().length() > 0) {
                        // 有别名
                        aggrColumns.put(item.getAlias(), mergeType);
                    } else {
                        // 修改添加别名  如果不加，jdbc方式时取不到正确结果
                        item.setAlias(method + i);
                        aggrColumns.put(method + i, mergeType);
                        isNeedChangeSql=true;
                    }
                    rrs.setHasAggrColumn(true);
                }
            } else {
                if (!(item.getExpr() instanceof SQLAllColumnExpr)) {
                    // 不是 select * from...
                    String alia = item.getAlias();
                    String field = getFieldName(item);

                    if (alia == null) {
                        alia = field;
                    }

                    aliaColumns.put(field, alia);
                }
            }

        }

        if(aggrColumns.size() > 0) {
            rrs.setMergeCols(aggrColumns);
        }

        boolean isDistinct=mysqlSelectQuery.getDistionOption()==2;
        if(isDistinct) {
            // 处理distinct关键字 通过优化转换成group by来实现  FIXME distinct消除相同项咋弄呢
            mysqlSelectQuery.setDistionOption(0);
            SQLSelectGroupByClause groupBy = new SQLSelectGroupByClause();
            for (String fieldName : aliaColumns.keySet()) {
                groupBy.addItem(new SQLIdentifierExpr(fieldName));
            }
            mysqlSelectQuery.setGroupBy(groupBy);
            isNeedChangeSql=true;
        }


        if(mysqlSelectQuery.getGroupBy() != null) {
            // 处理groupby
            List<SQLExpr> groupByItems = mysqlSelectQuery.getGroupBy().getItems();
            String[] groupByCols = buildGroupByCols(groupByItems,aliaColumns);
            rrs.setGroupByCols(groupByCols);
            rrs.setHavings(buildGroupByHaving(mysqlSelectQuery.getGroupBy().getHaving()));
            rrs.setHasAggrColumn(true);
        }

        if (isNeedChangeSql) {
            // 根据需要修改sql, toString中就会根据前面的修改生成新的sql
            String sql = stmt.toString();
            rrs.changeNodeSqlAfterAddLimit(schema, getCurentDbType(), sql, 0, -1, false);
            getCtx().setSql(sql);
        }
        return aliaColumns;
    }

    /* 获得having中的表达式 */
    private HavingCols buildGroupByHaving(SQLExpr having){
        if (having == null) {
            return null;
        }

        SQLBinaryOpExpr expr  = ((SQLBinaryOpExpr) having);
        SQLExpr left = expr.getLeft();
        SQLBinaryOperator operator = expr.getOperator();
        SQLExpr right = expr.getRight();

        String leftValue = null;;
        if (left instanceof SQLAggregateExpr) {
            // 如果做操作是聚合函数
            leftValue = ((SQLAggregateExpr) left).getMethodName() + "(" + ((SQLAggregateExpr) left).getArguments().get(0) + ")";

        } else if (left instanceof SQLIdentifierExpr) {
            leftValue = ((SQLIdentifierExpr) left).getName();
        }

        String rightValue = null;
        if (right instanceof  SQLNumericLiteralExpr) {
            rightValue = right.toString();
        } else if(right instanceof SQLTextLiteralExpr) {
            rightValue = StringUtil.removeBackquote(right.toString());
        }

        return new HavingCols(leftValue,rightValue,operator.getName());
    }

    /* TODO */
    private boolean isRoutMultiNode(SchemaConfig schema,  RouteResultset rrs)
    {
        if(rrs.getNodes()!=null&&rrs.getNodes().length>1)
        {
            return true;
        }
        LayerCachePool tableId2DataNodeCache = (LayerCachePool) MycatServer.getInstance().getCacheService().getCachePool("TableID2DataNodeCache");
        try
        {
            tryRoute(schema, rrs, tableId2DataNodeCache);
            if(rrs.getNodes()!=null&&rrs.getNodes().length>1)
            {
                return true;
            }
        } catch (SQLNonTransientException e)
        {
            throw new RuntimeException(e);
        }
        return false;
    }

    /* 获得字段名 */
    private String getFieldName(SQLSelectItem item){
        if (
            (item.getExpr() instanceof SQLPropertyExpr) ||
            (item.getExpr() instanceof SQLMethodInvokeExpr) ||
            (item.getExpr() instanceof SQLIdentifierExpr) ||
            (item.getExpr() instanceof SQLBinaryOpExpr)
            ) {
            return item.getExpr().toString();//字段别名
        } else {
            return item.toString();
        }
    }

    /* 获得一个sql语句的所有计算表达式 */
    private Map<String, Map<String, Set<ColumnRoutePair>>> getAllConditions() {
        Map<String, Map<String, Set<ColumnRoutePair>>> map = new HashMap<String, Map<String, Set<ColumnRoutePair>>>();
        for(RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
            if(unit != null && unit.getTablesAndConditions() != null) {
                map.putAll(unit.getTablesAndConditions());
            }
        }

        return map;
    }

    /* 根据ctx中的计算表达式,路由得到slice集合 */
    private void tryRoute(SchemaConfig schema, RouteResultset rrs, LayerCachePool cachePool) throws SQLNonTransientException
    {
        if(rrs.isFinishedRoute()) { return; }

        if(ctx.getTables() == null || ctx.getTables().size() == 0) {
            // 无表的select语句直接路由带任一节点
            rrs = RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), ctx.getSql());
            rrs.setFinishedRoute(true);
            return;
        }

        // 判断是否是全局表
        boolean isAllGlobalTable = RouterUtil.isAllGlobalTable(ctx, schema);

        // 获得路由后的slice集合
        SortedSet<RouteResultsetNode> nodeSet = new TreeSet<RouteResultsetNode>();
        for (RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
            RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, ctx, unit, rrs, true, cachePool);
            if (rrsTmp != null) {
                for (RouteResultsetNode node : rrsTmp.getNodes()) {
                    nodeSet.add(node);
                }
            }
            if(isAllGlobalTable) {
                // 都是全局表时只计算一遍路由
                break;
            }
        }

        if(nodeSet.size() == 0) {
            String msg = " find no Route:" + ctx.getSql();
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }

        // 将结果拷贝到rrs中
        RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSet.size()];
        int i = 0;
        for (Iterator<RouteResultsetNode> iterator = nodeSet.iterator(); iterator.hasNext();) {
            nodes[i] = (RouteResultsetNode) iterator.next();
            i++;
        }
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
    }


    protected String getCurentDbType() { return JdbcConstants.MYSQL; }


    /* 判断是否需要增加limit, 需要则生成新的sql,否则返回原先的sql */
    protected String getSql( RouteResultset rrs,SQLStatement stmt, boolean isNeedAddLimit) {
        if(getCurentDbType().equalsIgnoreCase("mysql") &&
                (isNeedChangeLimit(rrs) || isNeedAddLimit)) {
            return stmt.toString();
        }

        return getCtx().getSql();
    }


    /* 判断是否需要增加对分表下对limit的操作 */
    protected boolean isNeedChangeLimit(RouteResultset rrs) {
        if(rrs.getNodes() == null) {
            return false;
        } else {
            if(rrs.getNodes().length > 1) {
                return true;
            }
            return false;
        }
    }

    /* 判断是否需要缓存 */
    private boolean isNeedCache(SchemaConfig schema, RouteResultset rrs, MySqlSelectQueryBlock mysqlSelectQuery, Map<String, Map<String, Set<ColumnRoutePair>>> allConditions) {
        if(ctx.getTables() == null || ctx.getTables().size() == 0 ) {
            return false;
        }
        TableConfig tc = schema.getTables().get(ctx.getTables().get(0));
        if(tc==null || (ctx.getTables().size()==1 && tc.isGlobalTable())) {
            return false;
        } else {
            //单表主键查询
            if(ctx.getTables().size() == 1) {
                String tableName = ctx.getTables().get(0);
                String primaryKey = schema.getTables().get(tableName).getPrimaryKey();

                if(
                    ctx.getRouteCalculateUnit().getTablesAndConditions().get(tableName)!=null &&
                    ctx.getRouteCalculateUnit().getTablesAndConditions().get(tableName).get(primaryKey)!=null &&
                    tc.getDataNodes().size()>1
                  ) {
                    //有主键条件
                    return false;
                }
            }
            return true;
        }
    }

    /* 判断是否需要增加limit */
    private boolean isNeedAddLimit(SchemaConfig schema, RouteResultset rrs, MySqlSelectQueryBlock mysqlSelectQuery, Map<String, Map<String, Set<ColumnRoutePair>>> allConditions) {
        if(rrs.getLimitSize()>-1) {
            return false;

        } else if(schema.getDefaultMaxLimit() == -1) {
            return false;

        } else if (mysqlSelectQuery.getLimit() != null) {
            //语句中已有limit
            return false;

        } else if(ctx.getTables().size() == 1) {

            String tableName = ctx.getTables().get(0);
            TableConfig tableConfig = schema.getTables().get(tableName);
            if(tableConfig==null) {
                // 找不到则取schema的配置
                return schema.getDefaultMaxLimit() > -1;
            }

            boolean isNeedAddLimit= tableConfig.isNeedAddLimit();
            if(!isNeedAddLimit) {
                // 优先从配置文件取
                return false;
            }

            if(schema.getTables().get(tableName).isGlobalTable()) {
                return true;
            }

            String primaryKey = schema.getTables().get(tableName).getPrimaryKey();

            if(allConditions.get(tableName) == null) {
                //无条件
                return true;
            }

            if (allConditions.get(tableName).get(primaryKey) != null) {
                //条件中带主键
                return false;
            }

            return true;

        } else if(rrs.hasPrimaryKeyToCache() && ctx.getTables().size() == 1) {
            //只有一个表且条件中有主键,不需要limit了,因为主键只能查到一条记录
            return false;

        } else {
            //多表或无表
            return false;
        }
    }

    /* 获得字段的别名 */
    private String getAliaColumn(Map<String, String> aliaColumns,String column ){
        String alia=aliaColumns.get(column);

        if (alia==null) {
            if(column.indexOf(".") < 0) {
                String col = "." + column;
                String col2 = ".`" + column+"`";
                //展开aliaColumns，将<c.name,cname>之类的键值对展开成<c.name,cname>和<name,cname>
                for(Map.Entry<String, String> entry : aliaColumns.entrySet()) {
                    if(entry.getKey().endsWith(col)||entry.getKey().endsWith(col2)) {
                        if(entry.getValue() != null && entry.getValue().indexOf(".") > 0) {
                            return column;
                        }
                        return entry.getValue();
                    }
                }
            }

            return column;
        } else {
            return alia;
        }
    }

    /* 获得groupBy中的字段值*/
    private String[] buildGroupByCols(List<SQLExpr> groupByItems/*groupBy的字段值*/, Map<String, String> aliaColumns/*别名映射*/) {
        String[] groupByCols = new String[groupByItems.size()];
        for(int i= 0; i < groupByItems.size(); i++) {
            SQLExpr sqlExpr = groupByItems.get(i);
            String column;
            if(sqlExpr instanceof SQLIdentifierExpr ) {
                column=((SQLIdentifierExpr) sqlExpr).getName();

            } else {
                SQLExpr expr = ((MySqlSelectGroupByExpr) sqlExpr).getExpr();

                if (expr instanceof SQLName) {
                    //不要转大写 2015-2-10 sohudo StringUtil.removeBackquote(expr.getSimpleName().toUpperCase());
                    column = StringUtil.removeBackquote(((SQLName) expr).getSimpleName());

                } else {
                    column = StringUtil.removeBackquote(expr.toString());
                }
            }
            int dotIndex=column.indexOf(".") ;
            if(dotIndex!=-1)
            {
                //此步骤得到的column必须是不带.的，有别名的用别名，无别名的用字段名
                column=column.substring(dotIndex+1) ;
            }
            groupByCols[i] = getAliaColumn(aliaColumns,column);//column;
        }
        return groupByCols;
    }

    /* 获得orderBy的字段名 */
    protected LinkedHashMap<String/*字段别名*/, Integer/*递增或递减*/> buildOrderByCols(List<SQLSelectOrderByItem> orderByItems,Map<String, String> aliaColumns) {
        LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();

        for(int i= 0; i < orderByItems.size(); i++) {
            SQLOrderingSpecification type = orderByItems.get(i).getType();

            //orderColumn只记录字段名称,因为返回的结果集是不带表名的。
            SQLExpr expr =  orderByItems.get(i).getExpr();
            String col;
            if (expr instanceof SQLName) {
                col = ((SQLName)expr).getSimpleName();
            }
            else {
                col =expr.toString();
            }

            if(type == null) {
                type = SQLOrderingSpecification.ASC;
            }
            //此步骤得到的col必须是不带.的，有别名的用别名，无别名的用字段名
            col = getAliaColumn(aliaColumns,col);
            map.put(col, type == SQLOrderingSpecification.ASC ? OrderCol.COL_ORDER_TYPE_ASC : OrderCol.COL_ORDER_TYPE_DESC);
        }
        return map;
    }

    private boolean isConditionAlwaysTrue(SQLStatement statement) {
        SQLSelectStatement selectStmt = (SQLSelectStatement)statement;
        SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
        if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();
            SQLExpr expr = mysqlSelectQuery.getWhere();

            Object o = WallVisitorUtils.getValue(expr);
            if(Boolean.TRUE.equals(o)) {
                return true;
            }
            return false;
        } else {//union
            return false;
        }

    }

    protected void setLimitIFChange(SQLStatement stmt, RouteResultset rrs, SchemaConfig schema, SQLBinaryOpExpr one, int firstrownum, int lastrownum)
    {
        rrs.setLimitStart(firstrownum);
        rrs.setLimitSize(lastrownum - firstrownum);
        LayerCachePool tableId2DataNodeCache = (LayerCachePool) MycatServer.getInstance().getCacheService().getCachePool("TableID2DataNodeCache");
        try
        {
            tryRoute(schema, rrs, tableId2DataNodeCache);
        } catch (SQLNonTransientException e)
        {
            throw new RuntimeException(e);
        }
        if (isNeedChangeLimit(rrs))
        {
            one.setRight(new SQLIntegerExpr(0));
            String sql = stmt.toString();
            rrs.changeNodeSqlAfterAddLimit(schema,getCurentDbType(), sql,0,lastrownum, false);
            //设置改写后的sql
            getCtx().setSql(sql);
        }
    }
}