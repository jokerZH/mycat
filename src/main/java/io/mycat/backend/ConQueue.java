package io.mycat.backend;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

/* 后端连接池 */
public class ConQueue {
	private final ConcurrentLinkedQueue<BackendConnection> autoCommitCons = new ConcurrentLinkedQueue<BackendConnection>();
	private final ConcurrentLinkedQueue<BackendConnection> manCommitCons = new ConcurrentLinkedQueue<BackendConnection>();
	private long executeCount;

	/* 获得一个连接 */
	public BackendConnection takeIdleCon(boolean autoCommit) {
		ConcurrentLinkedQueue<BackendConnection> f1 = autoCommitCons;
		ConcurrentLinkedQueue<BackendConnection> f2 = manCommitCons;

		if (!autoCommit) {
			f1 = manCommitCons;
			f2 = autoCommitCons;

		}
		BackendConnection con = f1.poll();
		if (con == null || con.isClosedOrQuit()) {
			con = f2.poll();
		}
		if (con == null || con.isClosedOrQuit()) {
			return null;
		} else {
			return con;
		}
	}

	public long getExecuteCount() { return executeCount; }
	public void incExecuteCount() { this.executeCount++; }
	public ConcurrentLinkedQueue<BackendConnection> getAutoCommitCons() { return autoCommitCons; }
	public ConcurrentLinkedQueue<BackendConnection> getManCommitCons() { return manCommitCons; }

	public void removeCon(BackendConnection con) {
		if (!autoCommitCons.remove(con)) {
			manCommitCons.remove(con);
		}
	}

	/* 判断是不是同一个物理db的连接 */
	public boolean isSameCon(BackendConnection con) {
		if (autoCommitCons.contains(con)) {
			return true;
		} else if (manCommitCons.contains(con)) {
			return true;
		}
		return false;
	}

	/* 获得Count个连接 */
	public ArrayList<BackendConnection> getIdleConsToClose(int count) {
		ArrayList<BackendConnection> readyCloseCons = new ArrayList<BackendConnection>(count);

		while (!manCommitCons.isEmpty() && readyCloseCons.size() < count) {
			BackendConnection theCon = manCommitCons.poll();
			if (theCon != null) {
				readyCloseCons.add(theCon);
			}
		}

		while (!autoCommitCons.isEmpty() && readyCloseCons.size() < count) {
			BackendConnection theCon = autoCommitCons.poll();
			if (theCon != null) {
				readyCloseCons.add(theCon);
			}
		}

		return readyCloseCons;
	}

}
