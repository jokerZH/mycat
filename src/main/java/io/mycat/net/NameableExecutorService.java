package io.mycat.net;

import java.util.concurrent.ExecutorService;

public interface NameableExecutorService extends ExecutorService {
	public String getName();
}