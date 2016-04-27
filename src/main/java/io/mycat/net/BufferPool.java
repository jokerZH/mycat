package io.mycat.net;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* 多线程的buffer pool */
public final class BufferPool {
	private static final Logger LOGGER = LoggerFactory.getLogger(BufferPool.class);

	// this value not changed ,isLocalCacheThread use it
	public static final String LOCAL_BUF_THREAD_PREX = "$_";
	private final int chunkSize;	/* byteBuffer的大小 */
	private final ThreadLocalBufferPool localBufferPool;	/* 本地线程的 Buffer Pool */
	private final ConcurrentLinkedQueue<ByteBuffer> items = new ConcurrentLinkedQueue<ByteBuffer>();	/* 保存全部buffer */

	/**
	 * 只用于Connection读取Socket事件，每个Connection一个ByteBuffer（Direct），
	 * 此ByteBufer通常应该能容纳2-N个应用消息的报文长度，
	 * 对于超出的报文长度，则由BufferPool单独份分配临时的堆内ByteBuffer
	 */
	private final int conReadBuferChunk;	/* TODO 什么时候分配读的Buffer */
	private final ConcurrentLinkedQueue<ByteBuffer> conReadBuferQueue = new ConcurrentLinkedQueue<ByteBuffer>(); /* TODO */

	private long sharedOptsCount;			/* buffer回收到到item上的次数 */
	private int newCreated;					/* 初始化之后,新创建的byteBuffer */
	private final long threadLocalCount;	/* localBufferPool中的buffer个数*/
	private final long capactiy;			/* buffer的总个数 */

	/**
	 * <p>功能描述：构造函数 </p>
	 *
	 * @param bufferSize	Buffer Pool总大小
	 * @param chunkSize		单个byteBuffer的大小
	 * @param conReadBuferChunk		TODO
	 * @param threadLocalPercent	localBufferPool占总pool的比例
	 */
	public BufferPool(long bufferSize, int chunkSize, int conReadBuferChunk, int threadLocalPercent) {
		this.chunkSize = chunkSize;
		this.conReadBuferChunk = conReadBuferChunk;
		long size = bufferSize / chunkSize;
		size = (bufferSize % chunkSize == 0) ? size : size + 1;
		this.capactiy = size;
		threadLocalCount = threadLocalPercent * capactiy / 100;
		for (int i = 0; i < capactiy; i++) {
			items.offer(createDirectBuffer(chunkSize));
		}
		localBufferPool = new ThreadLocalBufferPool(threadLocalCount);
	}

	/* TODO */
	private static final boolean isLocalCacheThread() {
		final String thname = Thread.currentThread().getName();
		return (thname.length() < LOCAL_BUF_THREAD_PREX.length()) ? false
				: (thname.charAt(0) == '$' && thname.charAt(1) == '_');

	}

	public int getConReadBuferChunk() { return conReadBuferChunk; }
	public int getChunkSize() { return chunkSize; }
	public long getSharedOptsCount() { return sharedOptsCount; }
	public long size() { return this.items.size(); }
	public long capacity() { return capactiy + newCreated; }

	/**
	 * <p>功能描述：从conReadBufferQueue分配buffer,如果没有 直接新建 </p>
	 *
	 * @return ByteBuffer
	 */
	public ByteBuffer allocateConReadBuffer() {
		ByteBuffer result = conReadBuferQueue.poll();
		if (result != null) {
			return result;
		} else {
			return createDirectBuffer(conReadBuferChunk);
		}

	}

	public BufferArray allocateArray() {
		return new BufferArray(this);
	}

	/**
	 * <p>功能描述：分配一个byte buffer </p>
	 *
	 * @return ByteBuffer
	 */
	public ByteBuffer allocate() {
		ByteBuffer node = null;
		if (isLocalCacheThread()) {
			// allocate from threadlocal
			node = localBufferPool.get().poll();
			if (node != null) {
				return node;
			}
		}

		//否则从items中分配
		node = items.poll();
		if (node == null) {
			newCreated++;
			node = this.createDirectBuffer(chunkSize);
		}
		return node;
	}

	private boolean checkValidBuffer(ByteBuffer buffer) {
		// 拒绝回收null和容量大于chunkSize的缓存
		if (buffer == null || !buffer.isDirect()) {
			return false;
		} else if (buffer.capacity() != chunkSize) {
			LOGGER.warn("cant' recycle  a buffer not equals my pool chunksize "
					+ chunkSize + "  he is " + buffer.capacity());
			throw new RuntimeException("bad size");

			// return false;
		}
		buffer.clear();
		return true;
	}

	/**
	 * <p>功能描述：回收用于read的buffer </p>
	 * <p>其他说明： </p>
	 *
	 * @date   16/4/22 下午3:48
	 * @param
	 *
	 * @return
	 * @throws
	 */
	public void recycleConReadBuffer(ByteBuffer buffer) {
		if (buffer == null || !buffer.isDirect()) {
			return;
		} else if (buffer.capacity() != conReadBuferChunk) {
			LOGGER.warn("cant' recycle  a buffer not equals my pool con read chunksize "
					+ buffer.capacity());
		} else {
			buffer.clear();
			this.conReadBuferQueue.add(buffer);
		}
	}

	public void recycle(ByteBuffer buffer) {
		if (!checkValidBuffer(buffer)) {
			return;
		}
		if (isLocalCacheThread()) {
			BufferQueue localQueue = localBufferPool.get();
			if (localQueue.snapshotSize() < threadLocalCount) {
				localQueue.put(buffer);
			} else {
				// recyle 3/4 thread local buffer
				// 将线程的buffer移动到items上
				items.addAll(localQueue.removeItems(threadLocalCount * 3 / 4));
				items.offer(buffer);
				sharedOptsCount++;
			}
		} else {
			sharedOptsCount++;
			items.offer(buffer);
		}

	}

	public boolean testIfDuplicate(ByteBuffer buffer) {
		for (ByteBuffer exists : items) {
			if (exists == buffer) {
				return true;
			}
		}
		return false;

	}

	/**
	 * <p>功能描述：创建一个bytebuffer </p>
	 *
	 * @param size	buffer的大小
	 *
	 * @return ByteBuffer
	 */
	private ByteBuffer createDirectBuffer(int size) {
		// for performance
		return ByteBuffer.allocateDirect(size);
	}

	/**
	 * <p>功能描述：申请一个bytebuffer </p>
	 *
	 * @param size	buffer的大小
	 *
	 * @return ByteBuffer
	 */
	public ByteBuffer allocate(int size) {
		if (size <= this.chunkSize) {
			return allocate();
		} else {
			LOGGER.warn("allocate buffer size large than default chunksize:"
					+ this.chunkSize + " he want " + size);
			throw new RuntimeException("execuddd");
			// return createTempBuffer(size);
		}
	}
}
