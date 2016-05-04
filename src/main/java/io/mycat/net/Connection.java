package io.mycat.net;

import io.mycat.util.TimeUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 表示连接 负责读取一个完整的数据包
 */
public abstract class Connection implements ClosableConnection{
	public static Logger LOGGER = LoggerFactory.getLogger(Connection.class);

	protected String host;		/* ip 		*/
	protected int port;			/* port */
	protected int localPort;	/* 本地port */
	protected long id;			/* 连接ID 	*/

	private State state = State.connecting;	/* 当前连接的状态 */
	public enum State {
		connecting,	/* 正在建立连接 */
		connected, 	/* 连接已经建立 */
		closing,	/* 正在关闭连接 */
		closed,		/* 连接已经关闭 */
		failed		/* 连接失败 TODO */
	}

	/* 连接的方向，in表示是客户端连接过来的，out表示自己作为客户端去连接对端Sever */
	public enum Direction { in, out }
	private Direction direction = Direction.in;

	protected final SocketChannel channel;	/* TCP socket */

	private SelectionKey processKey;		/* Select机制用到的 */
	private static final int OP_NOT_READ = ~SelectionKey.OP_READ;
	private static final int OP_NOT_WRITE = ~SelectionKey.OP_WRITE;

	private ByteBuffer readBuffer;	/* 用于保存网络读取的数据 */
	private int readBufferOffset;	/* 在readerBuffer中的偏移 */

	private ByteBuffer writeBuffer;	/* 用户保存向网络写的数据 */
	private final ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<ByteBuffer>();	/* 用户保存向网络写的数据 */
	private final ReentrantLock writeQueueLock = new ReentrantLock();

	/* state */
	private long lastLargeMessageTime;
	protected boolean isClosed;
	protected boolean isSocketClosed;

	/* static */
	protected long startupTime;
	protected long lastReadTime;
	protected long lastWriteTime;
	protected int netInBytes;
	protected int netOutBytes;
	protected int pkgTotalSize;			/* 读取的mysql数据包总大小 */
	protected int pkgTotalCount;		/* 读取的mysql数据包个数   */
	private long idleTimeout;			/* 判断连接是否空闲的时间长度 */
	private long lastPerfCollectTime;

	@SuppressWarnings("rawtypes")
	protected NIOHandler handler;	/* 负责处理数据,这里只负责读取完整的数据包 */
	private int maxPacketSize;		/* TODO */
	private int packetHeaderSize;	/* TODO */

	public Connection(SocketChannel channel) {
		this.channel = channel;
		this.isClosed = false;
		this.startupTime = TimeUtil.currentTimeMillis();
		this.lastReadTime = startupTime;
		this.lastWriteTime = startupTime;
		this.lastPerfCollectTime = startupTime;
	}

	public void resetPerfCollectTime() {
		netInBytes = 0;
		netOutBytes = 0;
		pkgTotalCount = 0;
		pkgTotalSize = 0;
		lastPerfCollectTime = TimeUtil.currentTimeMillis();
	}

	public long getLastPerfCollectTime() { return lastPerfCollectTime; }
	public long getIdleTimeout() { return idleTimeout; }
	public void setIdleTimeout(long idleTimeout) { this.idleTimeout = idleTimeout; }
	public String getHost() { return host; }
	public void setHost(String host) { this.host = host; }
	public int getPort() { return port; }
	public void setPort(int port) { this.port = port; }
	public long getId() { return id; }
	public int getLocalPort() { return localPort; }
	public void setLocalPort(int localPort) { this.localPort = localPort; }
	public void setId(long id) { this.id = id; }
	public SocketChannel getChannel() { return channel; }
	public long getStartupTime() { return startupTime; }
	public long getLastReadTime() { return lastReadTime; }
	public long getLastWriteTime() { return lastWriteTime; }
	public long getNetInBytes() { return netInBytes; }
	public long getNetOutBytes() { return netOutBytes; }
	public ByteBuffer getReadBuffer() { return readBuffer; }
	public void setHandler(NIOHandler<? extends Connection> handler) { this.handler = handler; }
	@SuppressWarnings("rawtypes")
	public NIOHandler getHandler() { return this.handler; }
	public State getState() { return state; }
	public void setState(State newState) { this.state = newState; }
	public Direction getDirection() { return direction; }
	public void setDirection(Connection.Direction in) { this.direction = in; }
	public int getPkgTotalSize() { return pkgTotalSize; }
	public int getPkgTotalCount() { return pkgTotalCount; }
	public void setMaxPacketSize(int maxPacketSize) { this.maxPacketSize = maxPacketSize; }
	public void setPacketHeaderSize(int packetHeaderSize) { this.packetHeaderSize = packetHeaderSize; }



	/* 分配一个byte buffer */
	private ByteBuffer allocate() {
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate();
		return buffer;
	}

	/* 回收一个byter buffe */
	private final void recycle(ByteBuffer buffer) {
		NetSystem.getInstance().getBufferPool().recycle(buffer);
	}


	/* 判断当前连接是否是空闲的 */
	public boolean isIdleTimeout() {
		return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime,
				lastReadTime) + idleTimeout;

	}

	/* 处理读取的数据 */
	@SuppressWarnings("unchecked")
	public void handle(final ByteBuffer data, final int start,
			final int readedLength) {
		handler.handle(this, data, start, readedLength);
	}

	/**
	 * 读取一个完整的package，并调用处理函数
	 * 
	 * @param got	读取的数据大小
	 * @throws IOException
	 */
	public void onReadData(int got) throws IOException {
		if (isClosed) {
			return;
		}
		lastReadTime = TimeUtil.currentTimeMillis();
		if (got < 0) {
			this.close("stream closed");
			return;
		} else if (got == 0) {
			if (!this.channel.isOpen()) {
				this.close("socket closed");
				return;
			}
		}

		// static
		netInBytes += got;
		NetSystem.getInstance().addNetInBytes(got);

		// 循环处理字节信息
		int offset = readBufferOffset, length = 0, position = readBuffer.position();
		for (;;) {

			// 读物mysql数据包的长度
			length = getPacketLength(readBuffer, offset, position);
			if (length == -1) {
				// buffer中的数据不够一个header
				if (offset != 0) {
					this.readBuffer = compactReadBuffer(readBuffer, offset);
				} else if (!readBuffer.hasRemaining()) {
					throw new RuntimeException(
							"invalid readbuffer capacity ,too little buffer size "
									+ readBuffer.capacity());
				}
				break;
			}

			// static
			pkgTotalCount++;
			pkgTotalSize += length;

			// 尝试处理数据包
			if (offset + length <= position) {
				//一个完整的mysql数据包在Buffer中, 处理
				readBuffer.position(offset);
				// 处理一个包的数据
				handle(readBuffer, offset, length);

				offset += length;

				if (position == offset) {
					// 到达Buffer的末尾
					if (!readBuffer.isDirect()
							&& lastLargeMessageTime < lastReadTime - 30 * 1000L) {
						// 如果当前Buffer是超大Buffer,并且30秒内没有接收到超大数据包,就释放这个Buffer,换一个普通的Buffer

						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("change to direct con read buffer ,cur temp buf size :"
									+ readBuffer.capacity());
						}
						recycle(readBuffer);
						readBuffer = NetSystem.getInstance().getBufferPool().allocateConReadBuffer();

					} else {
						// 清空数据
						readBuffer.clear();
					}
					// no more data ,break
					readBufferOffset = 0;
					break;
				} else {
					// buffer中还有数据,尝试继续处理
					readBufferOffset = offset;
					readBuffer.position(position);
					continue;
				}
			} else {
				// buffer没有包含整个数据包,保证Buffer有足够的空间放下接下来的数据包
				if (!readBuffer.hasRemaining()) {
					readBuffer = ensureFreeSpaceOfReadBuffer(readBuffer,
							offset, length);
				}
				break;
			}
		}
	}

	public boolean isConnected() {
		return (this.state == Connection.State.connected);
	}

	private boolean isConReadBuffer(ByteBuffer buffer) {
		return buffer.capacity() == NetSystem.getInstance().getBufferPool().getConReadBuferChunk() && buffer.isDirect();
	}

	/* 扩展buffer，保证能够容下一个package */
	private ByteBuffer ensureFreeSpaceOfReadBuffer(ByteBuffer buffer,
			int offset, final int pkgLength) {
		// need a large buffer to hold the package
		if (pkgLength > maxPacketSize) {
			throw new IllegalArgumentException("Packet size over the limit.");
		} else if (buffer.capacity() < pkgLength) {
			ByteBuffer newBuffer = NetSystem.getInstance().getBufferPool()
					.allocate(pkgLength);
			lastLargeMessageTime = TimeUtil.currentTimeMillis();
			buffer.position(offset);
			newBuffer.put(buffer);
			readBuffer = newBuffer;
			if (isConReadBuffer(buffer)) {
				NetSystem.getInstance().getBufferPool()
						.recycleConReadBuffer(buffer);
			} else {
				recycle(buffer);
			}
			readBufferOffset = 0;
			return newBuffer;

		} else {
			if (offset != 0) {
				// compact bytebuffer only
				return compactReadBuffer(buffer, offset);
			} else {
				throw new RuntimeException(" not enough space");
			}
		}
	}

	/* 将buffer中前部的无用数据删除 */
	private ByteBuffer compactReadBuffer(ByteBuffer buffer, int offset) {
		buffer.limit(buffer.position());
		buffer.position(offset);
		buffer = buffer.compact();
		readBufferOffset = 0;
		return buffer;
	}

	/* 将byte[]封装成buffer，并放入write buffer list中 */
	public void write(byte[] src) {
		try {
			writeQueueLock.lock();
			ByteBuffer buffer = this.allocate();
			int offset = 0;
			int remains = src.length;
			while (remains > 0) {
				int writeable = buffer.remaining();
				if (writeable >= remains) {
					// can write whole srce
					buffer.put(src, offset, remains);
					this.writeQueue.offer(buffer);
					break;
				} else {
					// can write partly
					buffer.put(src, offset, writeable);
					offset += writeable;
					remains -= writeable;
					writeQueue.offer(buffer);
					buffer = allocate();
					continue;
				}

			}
		} finally {
			writeQueueLock.unlock();
		}
		this.enableWrite(true);
	}

	/* 将byte[]封装成buffer，并放入write buffer list中 */
	public final void write(ByteBuffer buffer, int from, int length) {
		try {
			writeQueueLock.lock();
			buffer.position(from);
			int remainByts = length;
			while (remainByts > 0) {
				ByteBuffer newBuf = allocate();
				int batchSize = newBuf.capacity();
				for (int i = 0; i < batchSize & remainByts > 0; i++) {
					newBuf.put(buffer.get());
					remainByts--;
				}
				writeQueue.offer(newBuf);
			}
		} finally {
			writeQueueLock.unlock();
		}
		this.enableWrite(true);

	}

	/* 将buffer放入wrier buffer list中 */
	public final void write(ByteBuffer buffer) {
		try {
			writeQueueLock.lock();
			writeQueue.offer(buffer);
		} finally {
			writeQueueLock.unlock();
		}
		this.enableWrite(true);
	}

	/* 关闭当前的连接 */
	@SuppressWarnings("unchecked")
	public void close(String reason) {
		if (!isClosed) {
			closeSocket();
			this.cleanup();
			isClosed = true;
			NetSystem.getInstance().removeConnection(this);
			LOGGER.info("close connection,reason:" + reason + " ," + this);
			if (handler != null) {
				handler.onClosed(this, reason);
			}
		}
	}

	/* 使用多线程异步实现关闭连接 */
	public void asynClose(final String reason) {
		Runnable runn = new Runnable() {
			public void run() {
				Connection.this.close(reason);
			}
		};
		NetSystem.getInstance().getTimer().schedule(runn, 1, TimeUnit.SECONDS);

	}

	/* 返回当前连接是否关闭 */
	public boolean isClosed() {
		return isClosed;
	}

    /* 如果判定当前连接是空闲的,就释放当前连接 */
	public void idleCheck() {
		if (isIdleTimeout()) {
			LOGGER.info(toString() + " idle timeout");
			close(" idle ");
		}
	}

	/* 清理资源 */
	protected void cleanup() {

		// 清理资源占用
		if (readBuffer != null) {
			if (isConReadBuffer(readBuffer)) {
				NetSystem.getInstance().getBufferPool().recycleConReadBuffer(readBuffer);
			} else {
				this.recycle(readBuffer);
			}
			this.readBuffer = null;
			this.readBufferOffset = 0;
		}
		if (writeBuffer != null) {
			recycle(writeBuffer);
			this.writeBuffer = null;
		}

		ByteBuffer buffer = null;
		while ((buffer = writeQueue.poll()) != null) {
			recycle(buffer);
		}
	}

	/* 获得buffer中头部的包的长度 */
	protected final int getPacketLength(ByteBuffer buffer, int offset,
			int position) {
		if (position < offset + packetHeaderSize) {
			return -1;
		} else {
			int length = buffer.get(offset) & 0xff;
			length |= (buffer.get(++offset) & 0xff) << 8;
			length |= (buffer.get(++offset) & 0xff) << 16;
			return length + packetHeaderSize;
		}
	}

	public ConcurrentLinkedQueue<ByteBuffer> getWriteQueue() {
		return writeQueue;
	}

	/* 将当前连接注册到selector中,监听读事件 */
	@SuppressWarnings("unchecked")
	public void register(Selector selector) throws IOException {
		processKey = channel.register(selector, SelectionKey.OP_READ, this);
		NetSystem.getInstance().addConnection(this);
		readBuffer = NetSystem.getInstance().getBufferPool().allocateConReadBuffer();
		this.handler.onConnected(this);
	}

	/* select中可写调用,尝试写,并根据是否还是数据,设置是否监听写事件 */
	public void doWriteQueue() {
		try {
			boolean noMoreData = write0();
			lastWriteTime = TimeUtil.currentTimeMillis();
			if (noMoreData && writeQueue.isEmpty()) {
				if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) != 0)) {
					disableWrite();
				}

			} else {

				if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) == 0)) {
					enableWrite(false);
				}
			}

		} catch (IOException e) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("caught err:", e);
			}
			close("err:" + e);
		}

	}

	/* 讲BufferArray中的Buffer加入到当前连接的writer Buffer list中 */
	public void write(BufferArray bufferArray) {
		try {
			writeQueueLock.lock();
			List<ByteBuffer> blockes = bufferArray.getWritedBlockLst();
			if (!bufferArray.getWritedBlockLst().isEmpty()) {
				for (ByteBuffer curBuf : blockes) {
					writeQueue.offer(curBuf);
				}
			}
			ByteBuffer curBuf = bufferArray.getCurWritingBlock();
			if (curBuf.position() == 0) {// empty
				this.recycle(curBuf);
			} else {
				writeQueue.offer(curBuf);
			}
		} finally {
			writeQueueLock.unlock();
			bufferArray.clear();
		}
		this.enableWrite(true);

	}

	/*
	 * nonblock的写writer Buffer list的数据
	 * true  写玩所有writer Buffer
	 * false 没有写完
	 */
	private boolean write0() throws IOException {

		int written = 0;
		ByteBuffer buffer = writeBuffer;
		if (buffer != null) {
			// 把上次没有写完的Buffer写了
			while (buffer.hasRemaining()) {
				written = channel.write(buffer);
				if (written > 0) {
					netOutBytes += written;
					NetSystem.getInstance().addNetOutBytes(written);
				} else {
					break;
				}
			}

			if (buffer.hasRemaining()) {
				return false;
			} else {
				writeBuffer = null;
				recycle(buffer);
			}
		}

		//写writer Buffer list
		while ((buffer = writeQueue.poll()) != null) {
			if (buffer.limit() == 0) {
				recycle(buffer);
				close("quit send");
				return true;
			}
			buffer.flip();
			while (buffer.hasRemaining()) {
				written = channel.write(buffer);
				if (written > 0) {
					netOutBytes += written;
					NetSystem.getInstance().addNetOutBytes(written);
					lastWriteTime = TimeUtil.currentTimeMillis();
				} else {
					break;
				}
			}
			if (buffer.hasRemaining()) {
				writeBuffer = buffer;
				return false;
			} else {
				recycle(buffer);
			}
		}
		return true;
	}

	private void disableWrite() {
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() & OP_NOT_WRITE);
		} catch (Exception e) {
			LOGGER.warn("can't disable write " + e + " con " + this);
		}

	}

	/**
	 * <p>功能描述：将当前conn设置成可写 </p>
	 * @param wakeup	是否让selector的wait返回,执行一次网络操作
	 */
	public void enableWrite(boolean wakeup) {
		boolean needWakeup = false;
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
			needWakeup = true;
		} catch (Exception e) {
			LOGGER.warn("can't enable write " + e);

		}
		if (needWakeup && wakeup) {
			processKey.selector().wakeup();
		}
	}

	public void disableRead() {
		SelectionKey key = this.processKey;
		key.interestOps(key.interestOps() & OP_NOT_READ);
	}

	public void enableRead() {
		boolean needWakeup = false;
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() | SelectionKey.OP_READ);
			needWakeup = true;
		} catch (Exception e) {
			LOGGER.warn("enable read fail " + e);
		}
		if (needWakeup) {
			processKey.selector().wakeup();
		}
	}


	/**
	 * 异步读取数据,在selector中调用
	 */
	protected void asynRead() throws IOException {
		if (this.isClosed) {
			return;
		}
		int got = channel.read(readBuffer);
		onReadData(got);
	}

	/* 关闭连接 */
	private void closeSocket() {
		if (channel != null) {
			boolean isSocketClosed = true;
			try {
				processKey.cancel();
				channel.close();
			} catch (Throwable e) {
			}
			boolean closed = isSocketClosed && (!channel.isOpen());
			if (closed == false) {
				LOGGER.warn("close socket of connnection failed " + this);
			}

		}
	}


	@Override
	public String toString() {
		return "Connection [host=" + host + ",  port=" + port + ", id=" + id
				+ ", state=" + state + ", direction=" + direction
				+ ", startupTime=" + startupTime + ", lastReadTime="
				+ lastReadTime + ", lastWriteTime=" + lastWriteTime + "]";
	}
}
