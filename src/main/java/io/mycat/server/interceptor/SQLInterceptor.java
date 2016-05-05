package io.mycat.server.interceptor;

/* used for interceptor sql before execute ,can modify sql befor execute */
public interface SQLInterceptor {

	/* return new sql to handler,ca't modify sql's type */
	String/*new sql*/ interceptSQL(String sql ,int sqlType);
}
