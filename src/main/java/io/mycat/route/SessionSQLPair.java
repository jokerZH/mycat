package io.mycat.route;

import io.mycat.server.NonBlockingSession;
import io.mycat.server.config.node.SchemaConfig;

/* session和sql的对 */
public class SessionSQLPair {
	public final NonBlockingSession session;
	
	public final SchemaConfig schema;	/* 逻辑db */
	public final String sql;			/* sql语句 */
	public final int type;				/* sql类型 */

	public SessionSQLPair(NonBlockingSession session, SchemaConfig schema, String sql,int type) {
		super();
		this.session = session;
		this.schema = schema;
		this.sql = sql;
		this.type=type;
	}
}
