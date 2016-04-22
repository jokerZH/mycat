package io.mycat.net;


/* 线程独立的byteBuffer pool */
public class ThreadLocalBufferPool extends ThreadLocal<BufferQueue> {
	private final long size;

	/* 设置buffer pool的大小 */
	public ThreadLocalBufferPool(long size) {
		this.size = size;
	}

	/* 初始化 */
	protected synchronized BufferQueue initialValue() {
		return new BufferQueue(size);
	}
}
