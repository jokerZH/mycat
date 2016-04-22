package io.mycat.net;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/* ByteBuffer的队列 */
public final class BufferQueue {
	private final long total;	/* 队列最大的byteBuffer的个数 */
	private final LinkedList<ByteBuffer> items = new LinkedList<ByteBuffer>(); /* ByteBuffer链表 */

	public BufferQueue(long capacity) {
		this.total = capacity;
	}

	/* for statics */
	public long snapshotSize() {
		return this.items.size();
	}

	/* 从链表中获得count个ByteBuffer */
	public Collection<ByteBuffer> removeItems(long count) {
		ArrayList<ByteBuffer> removed = new ArrayList<ByteBuffer>();
		Iterator<ByteBuffer> itor = items.iterator();
		while (itor.hasNext()) {
			removed.add(itor.next());
			itor.remove();
			if (removed.size() >= count) {
				break;
			}
		}
		return removed;
	}

	/* 将buffer加入到链表中 */
	public void put(ByteBuffer buffer) {
		this.items.offer(buffer);
		if (items.size() > total) {
			throw new java.lang.RuntimeException(
				"bufferQueue size exceeded ,maybe sql returned too many records ,cursize:" + items.size()
			);
		}
	}

	/* 获得一个byteBuffer */
	public ByteBuffer poll() {
		ByteBuffer buf = items.poll();
		return buf;
	}

	/* 返回是否为空 */
	public boolean isEmpty() {
		return items.isEmpty();
	}
}