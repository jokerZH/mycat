package io.mycat.sqlengine.mpp;

import java.io.Serializable;

/* 表示groupBy中的having语句, having a>0 */
public class HavingCols implements Serializable {
	String left;			/* 左操作 */
	String right;			/* 右操作 */
	String operator;		/* 操作 > = < 等 */
	public ColMeta colMeta;	/* 字段类型和下标 */

	public HavingCols(String left, String right, String operator) {
		this.left = left;
		this.right = right;
		this.operator = operator;
	}

	public String getLeft() { return left; }
	public void setLeft(String left) { this.left = left; }
	public String getRight() { return right; }
	public void setRight(String right) { this.right = right; }
	public String getOperator() { return operator; }
	public void setOperator(String operator) { this.operator = operator; }
	public ColMeta getColMeta() { return colMeta; }
	public void setColMeta(ColMeta colMeta) { this.colMeta = colMeta; }
}
