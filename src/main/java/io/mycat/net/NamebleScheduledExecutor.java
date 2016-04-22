package io.mycat.net;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

/* 定时执行的线程池 */
public class NamebleScheduledExecutor extends ScheduledThreadPoolExecutor
		implements NameableExecutorService {
	private final String name;

	public NamebleScheduledExecutor(String name, int corePoolSize,
			ThreadFactory threadFactory) {
		super(corePoolSize, threadFactory);
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

}
