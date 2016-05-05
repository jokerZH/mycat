package io.mycat.route;

import io.mycat.MycatServer;
import io.mycat.net.BufferArray;
import io.mycat.net.NetSystem;
import io.mycat.route.parser.druid.DruidSequenceHandler;
import io.mycat.server.ErrorCode;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.packet.EOFPacket;
import io.mycat.server.packet.FieldPacket;
import io.mycat.server.packet.ResultSetHeaderPacket;
import io.mycat.server.packet.RowDataPacket;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/* sequence的处理器 */
public class MyCATSequnceProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(MyCATSequnceProcessor.class);

	private LinkedBlockingQueue<SessionSQLPair> seqSQLQueue = new LinkedBlockingQueue<SessionSQLPair>();
	private volatile boolean running = true;


	/* 通过mysql协议返回参数value */
	private void outRawData(MySQLFrontConnection sc, String value) {
		byte packetId = 0;
		int fieldCount = 1;
		BufferArray bufferArray = NetSystem.getInstance().getBufferPool().allocateArray();

		/* header package */
		ResultSetHeaderPacket headerPkg = new ResultSetHeaderPacket();
		headerPkg.fieldCount = fieldCount;
		headerPkg.packetId = ++packetId;
		headerPkg.write(bufferArray);

		/* field package */
		FieldPacket fieldPkg = new FieldPacket();
		fieldPkg.packetId = ++packetId;
		fieldPkg.name = StringUtil.encode("SEQUNCE", sc.getCharset());
		fieldPkg.write(bufferArray);

		/* eof package */
		EOFPacket eofPckg = new EOFPacket();
		eofPckg.packetId = ++packetId;
		eofPckg.write(bufferArray);

		/* row data package */
		RowDataPacket rowDataPkg = new RowDataPacket(fieldCount);
		rowDataPkg.packetId = ++packetId;
		rowDataPkg.add(StringUtil.encode(value, sc.getCharset()));
		rowDataPkg.write(bufferArray);

		/* eof package */
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		lastEof.write(bufferArray);

		// write buffer
		sc.write(bufferArray);
	}

	/* 给pair中的sql加上sequence,然后调用正常的流程 */
	private void executeSeq(SessionSQLPair pair) {
		try {
			/*
			 * 扩展NodeToString实现自定义全局序列号 NodeToString strHandler = new
			 * ExtNodeToString4SEQ(MycatServer
			 * .getInstance().getConfig().getSystem() .getSequnceHandlerType());
			 * // 如果存在sequence 转化sequence为实际数值 String charset =
			 * pair.session.getSource().getCharset(); QueryTreeNode ast =
			 * SQLParserDelegate.parse(pair.sql, charset == null ? "utf-8" :
			 * charset); String sql = strHandler.toString(ast); if
			 * (sql.toUpperCase().startsWith("SELECT")) { String
			 * value=sql.substring("SELECT".length()).trim();
			 * outRawData(pair.session.getSource(),value); return; }
			 */

			// 使用Druid解析器实现sequence处理
			DruidSequenceHandler sequenceHandler = new DruidSequenceHandler(MycatServer.getInstance().getConfig().getSystem().getSequnceHandlerType());

			// 替换sequence
			String charset = pair.session.getSource().getCharset();
			String executeSql = sequenceHandler.getExecuteSql(pair.sql, charset == null ? "utf-8" : charset);

			/* 执行一般的sql流程 */
			pair.session.getSource().routeEndExecuteSQL(executeSql, pair.type, pair.schema);

		} catch (Exception e) {
			LOGGER.error("MyCATSequenceProcessor.executeSeq(SesionSQLPair)", e);
			pair.session.getSource().writeErrMessage(ErrorCode.ER_YES, "mycat sequnce err." + e);
			return;
		}
	}

	public void addNewSql(SessionSQLPair pair) { seqSQLQueue.add(pair); }
	public MyCATSequnceProcessor() { new ExecuteThread().start(); }
	public void shutdown() { running = false; }

	/* 异步执行seq语句 */
	class ExecuteThread extends Thread {
		public void run() {
			while (running) {
				try {
					SessionSQLPair pair = seqSQLQueue.poll(100, TimeUnit.MILLISECONDS);
					if (pair != null) {
						executeSeq(pair);
					}
				} catch (Exception e) {
					LOGGER.warn("MyCATSequenceProcessor$ExecutorThread", e);
				}
			}
		}
	}
}
