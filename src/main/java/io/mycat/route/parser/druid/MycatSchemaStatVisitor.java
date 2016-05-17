package io.mycat.route.parser.druid;

import io.mycat.route.util.RouterUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.stat.TableStat.Column;
import com.alibaba.druid.stat.TableStat.Condition;
import com.alibaba.druid.stat.TableStat.Mode;

/* Druid解析器中用来从ast语法中提取表名、条件、字段等的vistor, 主要获得逻辑表达式中的操作,以or操作为分隔符号 */
public class MycatSchemaStatVisitor extends MySqlSchemaStatVisitor {
	private boolean hasOrCondition = false;	/* 有or逻辑 */
	private List<WhereUnit> whereUnits = new CopyOnWriteArrayList<WhereUnit>();			/* 当前Where的表达式,根据or切分  */
	private List<WhereUnit> storedwhereUnits = new CopyOnWriteArrayList<WhereUnit>();	/* 切分好的where表达式 */
	
	private void reset() {
		this.conditions.clear();
		this.whereUnits.clear();
		this.hasOrCondition = false;
	}
	
	public List<WhereUnit> getWhereUnits() { return whereUnits; }
	public boolean hasOrCondition() { return hasOrCondition; }
	
    @Override
    public boolean visit(SQLSelectStatement x) {
        setAliasMap();
        return true;
    }

	/* 处理between */
    @Override
    public boolean visit(SQLBetweenExpr x) {
        String begin = null;
        if(x.beginExpr instanceof SQLCharExpr)
        {
            begin= (String) ( (SQLCharExpr)x.beginExpr).getValue();
        }  else {
            begin = x.beginExpr.toString();
        }
        String end = null;
        if(x.endExpr instanceof SQLCharExpr)
        {
            end= (String) ( (SQLCharExpr)x.endExpr).getValue();
        }  else {
            end = x.endExpr.toString();
        }
        Column column = getColumn(x);
        if (column == null) {
            return true;
        }

		// 保存结果的
        Condition condition = null;
        for (Condition item : this.getConditions()) {
            if (item.getColumn().equals(column) && item.getOperator().equals("between")) {
                condition = item;
                break;
            }
        }

        if (condition == null) {
            condition = new Condition();
            condition.setColumn(column);
            condition.setOperator("between");
            this.conditions.add(condition);
        }


        condition.getValues().add(begin);
        condition.getValues().add(end);


        return true;
    }

	/* 获得表达式中字段的表明和字段名 */
    @Override
    protected Column getColumn(SQLExpr expr) {
        Map<String, String> aliasMap = getAliasMap();
        if (aliasMap == null) {
            return null;
        }

        if (expr instanceof SQLPropertyExpr) {
            SQLExpr owner = ((SQLPropertyExpr) expr).getOwner();
            String column = ((SQLPropertyExpr) expr).getName();

            if (owner instanceof SQLIdentifierExpr) {
                String tableName = ((SQLIdentifierExpr) owner).getName();
                String table = tableName;
                if (aliasMap.containsKey(table)) {
                    table = aliasMap.get(table);
                }

                if (variants.containsKey(table)) {
                    return null;
                }

                if (table != null) {
                    return new Column(table, column);
                }

                return handleSubQueryColumn(tableName, column);
            }

            return null;
        }

        if (expr instanceof SQLIdentifierExpr) {
            Column attrColumn = (Column) expr.getAttribute(ATTR_COLUMN);
            if (attrColumn != null) {
                return attrColumn;
            }

            String column = ((SQLIdentifierExpr) expr).getName();
            String table = getCurrentTable();
            if (table != null && aliasMap.containsKey(table)) {
                table = aliasMap.get(table);
                if (table == null) {
                    return null;
                }
            }

            if (table != null) {
                return new Column(table, column);
            }

            if (variants.containsKey(column)) {
                return null;
            }

            return new Column("UNKNOWN", column);
        }

		/* A between B and C */
        if(expr instanceof SQLBetweenExpr) {
			// 查看Ａ是否是一个字段,是则返回, 否则返回null
            SQLBetweenExpr betweenExpr = (SQLBetweenExpr)expr;

            if(betweenExpr.getTestExpr() != null) {
                String tableName = null;
                String column = null;
                if(betweenExpr.getTestExpr() instanceof SQLPropertyExpr) {
					//字段带别名的
                    tableName = ((SQLIdentifierExpr)((SQLPropertyExpr) betweenExpr.getTestExpr()).getOwner()).getName();
                    column = ((SQLPropertyExpr) betweenExpr.getTestExpr()).getName();
					SQLObject query = this.subQueryMap.get(tableName);
					if(query == null) {
						if (aliasMap.containsKey(tableName)) {
							tableName = aliasMap.get(tableName);
						}
						// 返回表明 字段名
						return new Column(tableName, column);
					}
					// 跟子查询相关
                    return handleSubQueryColumn(tableName, column);
                } else if(betweenExpr.getTestExpr() instanceof SQLIdentifierExpr) {
					// 一般的字段
                    column = ((SQLIdentifierExpr) betweenExpr.getTestExpr()).getName();
                    //字段不带别名的,此处如果是多表，容易出现ambiguous，
                    //不知道这个字段是属于哪个表的,fdbparser用了defaultTable，即join语句的leftTable
                    tableName = getOwnerTableName(betweenExpr,column);
                }
                String table = tableName;
                if (aliasMap.containsKey(table)) {
                    table = aliasMap.get(table);
                }

                if (variants.containsKey(table)) {
                    return null;
                }

                if (table != null&&!"".equals(table)) {
                    return new Column(table, column);
                }
            }
        }


        return null;
    }

    @Override
    public boolean visit(MySqlDeleteStatement x) {
        setAliasMap();

        setMode(x, Mode.Delete);

        accept(x.getFrom());
        accept(x.getUsing());
        x.getTableSource().accept(this);

        if (x.getTableSource() instanceof SQLExprTableSource) {
            SQLName tableName = (SQLName) ((SQLExprTableSource) x.getTableSource()).getExpr();
            String ident = tableName.toString();
            setCurrentTable(x, ident);

            TableStat stat = this.getTableStat(ident,ident);
            stat.incrementDeleteCount();
        }

        accept(x.getWhere());

        accept(x.getOrderBy());
        accept(x.getLimit());

        return false;
    }

    @Override
    public void endVisit(MySqlDeleteStatement x) {}




    /* 从between语句中获取字段所属的表名 对于容易出现ambiguous的（字段不知道到底属于哪个表），实际应用中必须使用别名来避免歧义 */
    private String getOwnerTableName(SQLBetweenExpr betweenExpr/*between的结构体*/,String column/*字段名*/) {
        if(tableStats.size() == 1) {//只有一个表，直接返回这一个表名
            return tableStats.keySet().iterator().next().getName();
        } else if(tableStats.size() == 0) {//一个表都没有，返回空串
            return "";
        } else {//多个表名
            for(Column col : columns) {//从columns中找表名
                if(col.getName().equals(column)) {
                    return col.getTable();
                }
            }

            //前面没找到表名的，自己从parent中解析

            SQLObject parent = betweenExpr.getParent();
            if(parent instanceof SQLBinaryOpExpr)
            {
                parent=parent.getParent();
            }

            if(parent instanceof MySqlSelectQueryBlock) {
                MySqlSelectQueryBlock select = (MySqlSelectQueryBlock) parent;
                if(select.getFrom() instanceof SQLJoinTableSource) {//多表连接
                    SQLJoinTableSource joinTableSource = (SQLJoinTableSource)select.getFrom();
                    return joinTableSource.getLeft().toString();//将left作为主表，此处有不严谨处，但也是实在没有办法，如果要准确，字段前带表名或者表的别名即可
                } else if(select.getFrom() instanceof SQLExprTableSource) {//单表
                    return select.getFrom().toString();
                }
            }
            else if(parent instanceof SQLUpdateStatement) {
                SQLUpdateStatement update = (SQLUpdateStatement) parent;
                return update.getTableName().getSimpleName();
            } else if(parent instanceof SQLDeleteStatement) {
                SQLDeleteStatement delete = (SQLDeleteStatement) parent;
                return delete.getTableName().getSimpleName();
            } else {
                
            }
        }
        return "";
    }

	/* 处理二元操作 */
    @Override
	public boolean visit(SQLBinaryOpExpr x) {
        x.getLeft().setParent(x);
        x.getRight().setParent(x);

        switch (x.getOperator()) {
            case Equality:
            case LessThanOrEqualOrGreaterThan:
            case Is:
            case IsNot:
                handleCondition(x.getLeft(), x.getOperator().name, x.getRight());
                handleCondition(x.getRight(), x.getOperator().name, x.getLeft());
                handleRelationship(x.getLeft(), x.getOperator().name, x.getRight());
                break;
            case BooleanOr:
            	//永真条件，where条件抛弃
            	if(!RouterUtil.isConditionAlwaysTrue(x)) {
					// 将or相关的逻辑的结构合并起来,构建成一个WhereUnit结构
            		hasOrCondition = true;
            		
            		WhereUnit whereUnit = null;
            		if(conditions.size() > 0) {
            			whereUnit = new WhereUnit();
            			whereUnit.setFinishedParse(true);
            			whereUnit.addOutConditions(getConditions());
            			WhereUnit innerWhereUnit = new WhereUnit(x);
            			whereUnit.addSubWhereUnit(innerWhereUnit);
            		} else {
            			whereUnit = new WhereUnit(x);
            			whereUnit.addOutConditions(getConditions());
            		}
            		whereUnits.add(whereUnit);
            	}
            	return false;
            case Like:
            case NotLike:
            case NotEqual:
            case GreaterThan:
            case GreaterThanOrEqual:
            case LessThan:
            case LessThanOrEqual:
            default:
                break;
        }
        return true;
    }

	/* TODO */
	@Override
    public boolean visit(SQLAlterTableStatement x) {
        String tableName = x.getName().toString();
        TableStat stat = getTableStat(tableName,tableName);
        stat.incrementAlterCount();

        setCurrentTable(x, tableName);

        for (SQLAlterTableItem item : x.getItems()) {
            item.setParent(x);
            item.accept(this);
        }

        return false;
    }



	/* 分解条件 */
	public List<List<Condition>> splitConditions() {
		//按照or拆分
		for(WhereUnit whereUnit : whereUnits) {
			splitUntilNoOr(whereUnit);
		}
		
		this.storedwhereUnits.addAll(whereUnits);
		
		loopFindSubWhereUnit(whereUnits);
		
		//拆分后的条件块解析成Condition列表
		for(WhereUnit whereUnit : storedwhereUnits) {
			this.getConditionsFromWhereUnit(whereUnit);
		}
		
		//多个WhereUnit组合:多层集合的组合
		return mergedConditions();
	}
	
	/* 循环寻找子WhereUnit（实际是嵌套的or） TODO */
	private void loopFindSubWhereUnit(List<WhereUnit> whereUnitList) {
		List<WhereUnit> subWhereUnits = new ArrayList<WhereUnit>();

		for(WhereUnit whereUnit : whereUnitList) {
			if(whereUnit.getSplitedExprList().size() > 0) {
				List<SQLExpr> removeSplitedList = new ArrayList<SQLExpr>();

				for(SQLExpr sqlExpr : whereUnit.getSplitedExprList()) {
                    // TODO
					reset();
					if(isExprHasOr(sqlExpr)) {
                        // 其中有or操作
						removeSplitedList.add(sqlExpr);
						WhereUnit subWhereUnit = this.whereUnits.get(0);
						splitUntilNoOr(subWhereUnit);
						whereUnit.addSubWhereUnit(subWhereUnit);
						subWhereUnits.add(subWhereUnit);
					} else {
						this.conditions.clear();
					}
				}
				if(removeSplitedList.size() > 0) {
					whereUnit.getSplitedExprList().removeAll(removeSplitedList);
				}
			}
			subWhereUnits.addAll(whereUnit.getSubWhereUnit());
		}
		if(subWhereUnits.size() > 0) {
			loopFindSubWhereUnit(subWhereUnits);
		}
	}

    /* 判断expr中是否有or操作 */
	private boolean isExprHasOr(SQLExpr expr) {
		expr.accept(this);
		return hasOrCondition;
	}

    /* 将多个WhereUnit的二元操作合并  */
	private List<List<Condition>> mergedConditions() {
		if(storedwhereUnits.size() == 0) {
			return new ArrayList<List<Condition>>();
		}
		for(WhereUnit whereUnit : storedwhereUnits) {
			mergeOneWhereUnit(whereUnit);
		}
		return getMergedConditionList(storedwhereUnits);
	}
	
	/* 将一个WhereUnit中的Condition存放到它的conditionList中  */
	private void mergeOneWhereUnit(WhereUnit whereUnit) {
		if(whereUnit.getSubWhereUnit().size() > 0) {
			for(WhereUnit sub : whereUnit.getSubWhereUnit()) {
				mergeOneWhereUnit(sub);
			}
			
			if(whereUnit.getSubWhereUnit().size() > 1) {
				List<List<Condition>> mergedConditionList = getMergedConditionList(whereUnit.getSubWhereUnit());
				if(whereUnit.getOutConditions().size() > 0) {
					for(int i = 0; i < mergedConditionList.size() ; i++) {
						mergedConditionList.get(i).addAll(whereUnit.getOutConditions());
					}
				}
				whereUnit.setConditionList(mergedConditionList);
			} else if(whereUnit.getSubWhereUnit().size() == 1) {
				if(whereUnit.getOutConditions().size() > 0 && whereUnit.getSubWhereUnit().get(0).getConditionList().size() > 0) {
					for(int i = 0; i < whereUnit.getSubWhereUnit().get(0).getConditionList().size() ; i++) {
						whereUnit.getSubWhereUnit().get(0).getConditionList().get(i).addAll(whereUnit.getOutConditions());
					}
				}
				whereUnit.getConditionList().addAll(whereUnit.getSubWhereUnit().get(0).getConditionList());
			}
		} else {
			//do nothing
		}
	}
	
	/* 条件合并：多个WhereUnit中的条件组合Condition */
	private List<List<Condition>> getMergedConditionList(List<WhereUnit> whereUnitList) {
		List<List<Condition>> mergedConditionList = new ArrayList<List<Condition>>();
		if(whereUnitList.size() == 0) {
			return mergedConditionList; 
		}
		mergedConditionList.addAll(whereUnitList.get(0).getConditionList());
		
		for(int i = 1; i < whereUnitList.size(); i++) {
			mergedConditionList = merge(mergedConditionList, whereUnitList.get(i).getConditionList());
		}
		return mergedConditionList;
	}
	
	/* 两个list中的条件组合 */
	private List<List<Condition>> merge(List<List<Condition>> list1, List<List<Condition>> list2) {
		if(list1.size() == 0) {
			return list2;
		} else if (list2.size() == 0) {
			return list1;
		}
		
		List<List<Condition>> retList = new ArrayList<List<Condition>>();
		for(int i = 0; i < list1.size(); i++) {
			for(int j = 0; j < list2.size(); j++) {
				List<Condition> listTmp = new ArrayList<Condition>();
				listTmp.addAll(list1.get(i));
				listTmp.addAll(list2.get(j));
				retList.add(listTmp);
			}
		}
		return retList;
	}

    /* 将whereUnit中的二元操作存放在conditions中 */
	private void getConditionsFromWhereUnit(WhereUnit whereUnit) {
        // 保存各个or相关的二元操作
		List<List<Condition>> retList = new ArrayList<List<Condition>>();

		//or语句外层的条件:如where condition1 and (condition2 or condition3),condition1就会在外层条件中,因为之前提取
		List<Condition> outSideCondition = new ArrayList<Condition>();
		outSideCondition.addAll(conditions);

		this.conditions.clear();
		for(SQLExpr sqlExpr : whereUnit.getSplitedExprList()) {
			sqlExpr.accept(this);
			List<Condition> conditions = new ArrayList<Condition>();
			conditions.addAll(getConditions());
			conditions.addAll(outSideCondition);
			retList.add(conditions);
			this.conditions.clear();
		}
		whereUnit.setConditionList(retList);
		
		for(WhereUnit subWhere : whereUnit.getSubWhereUnit()) {
			getConditionsFromWhereUnit(subWhere);
		}
	}
	
	/* 递归拆分OR TODO:考虑嵌套or语句，条件中有子查询、 exists等很多种复杂情况是否能兼容 */
	private void splitUntilNoOr(WhereUnit whereUnit) {
		if(whereUnit.isFinishedParse()) {
			if(whereUnit.getSubWhereUnit().size() > 0) {
				for(int i = 0; i < whereUnit.getSubWhereUnit().size(); i++) {
                    // 往下递归的拆解
					splitUntilNoOr(whereUnit.getSubWhereUnit().get(i));
				}
			}
		} else {
            // TODO 难道底层的whereUnit都是没有解析完成的?
			SQLBinaryOpExpr expr = whereUnit.getCanSplitExpr();
			if(expr.getOperator() == SQLBinaryOperator.BooleanOr) {
                // 把右操作放入
				addExprIfNotFalse(whereUnit, expr.getRight());

				if(expr.getLeft() instanceof SQLBinaryOpExpr) {
                    // 二元表达式的左表达式还是一个二元操作
					whereUnit.setCanSplitExpr((SQLBinaryOpExpr)expr.getLeft());
                    // 递归的切分
					splitUntilNoOr(whereUnit);
				} else {
                    // 把左操作放入
					addExprIfNotFalse(whereUnit, expr.getLeft());
				}
			} else {
                // 不是or操作,直接放入
				addExprIfNotFalse(whereUnit, expr);
				whereUnit.setFinishedParse(true);
			}
		}
    }

    /* 将表达式放入whereUnit的切分完成队列中 */
	private void addExprIfNotFalse(WhereUnit whereUnit, SQLExpr expr) {
		if(!RouterUtil.isConditionAlwaysFalse(expr)) {
			//非永假条件加入路由计算
			whereUnit.addSplitedExpr(expr);
		}
	}
}
