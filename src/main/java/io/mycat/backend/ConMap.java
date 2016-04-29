package io.mycat.backend;

import io.mycat.net.Connection;
import io.mycat.net.NetSystem;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/* 单个后端节点的连接池的管理，按照key分开 */
public class ConMap {
	private final ConcurrentHashMap<String/*物理db名*/, ConQueue> items = new ConcurrentHashMap<String, ConQueue>();

	/* 获得某个db的所有连接 */
	public ConQueue getSchemaConQueue(String schema) {
		ConQueue queue = items.get(schema);
		if (queue == null) {
			ConQueue newQueue = new ConQueue();
			queue = items.putIfAbsent(schema, newQueue);
			return (queue == null) ? newQueue : queue;
		}
		return queue;
	}

	/* 尝试获得一个连接, 如果schema下的连接用完了,就从别的schema拿连接过来用,反正使用use dbName就可以切换了  */
	public BackendConnection tryTakeCon(final String schema, boolean autoCommit) {
		final ConQueue queue = items.get(schema);
		BackendConnection con = tryTakeCon(queue, autoCommit);
		if (con != null) {
			return con;
		} else {
			for (ConQueue queue2 : items.values()) {
				if (queue != queue2) {
					con = tryTakeCon(queue2, autoCommit);
					if (con != null) {
						return con;
					}
				}
			}
		}
		return null;

	}

	/* 尝试从queue中获得一个连接 */
	private BackendConnection tryTakeCon(ConQueue queue, boolean autoCommit) {
		BackendConnection con = null;
		if (queue != null && ((con = queue.takeIdleCon(autoCommit)) != null)) {
			return con;
		} else {
			return null;
		}
	}

	/* 获得所有连接 */
	public Collection<ConQueue> getAllConQueue() { return items.values(); }

	/* 返回某个db下,正在被使用的Conn的个数 */
	public int getActiveCountForSchema(String schema, PhysicalDatasource dataSouce) {
		int total = 0;
		for (Connection conn : NetSystem.getInstance().getAllConnectios().values()) {
			if (conn instanceof BackendConnection) {
				BackendConnection theCon = (BackendConnection) conn;
				if (theCon.getSchema().equals(schema) && theCon.getPool() == dataSouce) {
					if (theCon.isBorrowed()) {
						total++;
					}
				}
			}
		}
		return total;
	}

	/* 返回正在被使用的Conn的个数 */
	public int getActiveCountForDs(PhysicalDatasource dataSouce) {

		int total = 0;
		for (Connection conn : NetSystem.getInstance().getAllConnectios().values()) {
			if (conn instanceof BackendConnection) {
				BackendConnection theCon = (BackendConnection) conn;
				if (theCon.getPool() == dataSouce) {
					if (theCon.isBorrowed()) {
						total++;
					}
				}
			}
		}
		return total;
	}

	/* 清理当前实例下所有Conn */
	public void clearConnections(String reason, PhysicalDatasource dataSouce) {

		Iterator<Entry<Long, Connection>> itor = NetSystem.getInstance().getAllConnectios().entrySet().iterator();
		while (itor.hasNext()) {
			Entry<Long, Connection> entry = itor.next();
			Connection con = entry.getValue();
			if (con instanceof BackendConnection) {
				if (((BackendConnection) con).getPool() == dataSouce) {
					con.close(reason);
					itor.remove();
				}
			}

		}
        items.clear();
	}

}
