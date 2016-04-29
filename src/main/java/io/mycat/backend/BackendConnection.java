package io.mycat.backend;

import io.mycat.net.ClosableConnection;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.executors.ResponseHandler;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public interface BackendConnection extends ClosableConnection{
	public boolean isModifiedSQLExecuted();

	/* 是否是slave的连接 */
	public boolean isFromSlaveDB();

	public String getSchema();

	public void setSchema(String newSchema);

	public long getLastTime();

	public boolean isClosedOrQuit();

	/* 将某个对象辅助到Conn上 */
	public void setAttachment(Object attachment);

	public void quit();

	public void setLastTime(long currentTimeMillis);

	/* 将连接返回给连接池 */
	public void release();

	/* 设置处理回应数据包的handler */
	public void setResponseHandler(ResponseHandler commandHandler);

	public void commit();

	/* 发送sql语句到后端 */
	public void query(String sql) throws UnsupportedEncodingException;

	public Object getAttachment();

	// public long getThreadId();

	public void execute(RouteResultsetNode node, MySQLFrontConnection source, boolean autocommit) throws IOException;

	public boolean syncAndExcute();

	public void rollback();

	/* TODO 是否在使用 */
	public boolean isBorrowed();

	/* 表明连接正在被是哟给你 */
	public void setBorrowed(boolean borrowed);

	public int getTxIsolation();

	public boolean isAutocommit();

	public long getId();

	public void close(String reason);

	public String getCharset();

	public PhysicalDatasource getPool();
}
