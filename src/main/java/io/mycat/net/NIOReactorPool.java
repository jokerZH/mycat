package io.mycat.net;

import java.io.IOException;

/* 负责管理处理请求的线程 */
public class NIOReactorPool {
	private final NIOReactor[] reactors;	/* 每个reacotr有自己的NIO机制 */
	private volatile int nextReactor;		/* 处理下一个Conn的reactor的下标 */

	public NIOReactorPool(String name, int poolSize) throws IOException {
		reactors = new NIOReactor[poolSize];
		for (int i = 0; i < poolSize; i++) {
			NIOReactor reactor = new NIOReactor(name + "-" + i);
			reactors[i] = reactor;
			reactor.startup();
		}
	}

	public NIOReactor getNextReactor() {
        int i = ++nextReactor;
        if (i >= reactors.length) {
			i=nextReactor = 0;
		}
		return reactors[i];
	}
}
