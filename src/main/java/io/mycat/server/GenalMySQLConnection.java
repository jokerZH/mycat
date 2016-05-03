package io.mycat.server;

import io.mycat.net.BufferArray;
import io.mycat.net.Connection;
import io.mycat.net.NetSystem;
import io.mycat.server.packet.AuthPacket;
import io.mycat.server.packet.ErrorPacket;
import io.mycat.server.packet.HandshakePacket;
import io.mycat.server.packet.util.CharsetUtil;
import io.mycat.server.packet.util.SecurityUtil;

import java.io.UnsupportedEncodingException;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;

/*客户端连接 */
public class GenalMySQLConnection extends Connection {
	public static final int maxPacketSize = 16 * 1024 * 1024;
	protected final AtomicBoolean isQuit = new AtomicBoolean(false);
	protected byte[] seed;								/* 权限表中的seed */
	protected String user;								/* 用户名 */
	protected String password;							/* 密码 */
	protected volatile String schema;					/* 逻辑LDBName */
	protected volatile int txIsolation;					/* mysql的隔离级别 */
	protected volatile boolean autocommit;				/* 是否自动提交 */
	protected volatile boolean txInterrupted;			/* TODO */
	protected volatile String txInterrputMsg = "";		/* TODO */
	protected long lastInsertId;						/* 最新的自增id */
	protected volatile String oldSchema;				/* 老的逻辑LDBName */
	protected long clientFlags;							/* TODO 客户端的flag */
	protected boolean isAccepted;						/* TODO 连接是否建立 */
	protected boolean isAuthenticated;					/* 是否已经通过认证 */
	protected volatile String charset;					/* 字符集 */
	protected volatile int charsetIndex;				/* 字符集下标 */
	protected HandshakePacket handshake;				/* 发给客户端的第一个权限包 */
	protected boolean isSupportCompress = false;		/* 是否压缩 */

	public GenalMySQLConnection(SocketChannel channel) {
		super(channel);
		this.txInterrupted = false;
		this.autocommit = true;
	}

	public int getTxIsolation() { return txIsolation; }
	public void setTxIsolation(int txIsolation) { this.txIsolation = txIsolation; }
	public boolean isAutocommit() { return autocommit; }
	public boolean isAuthenticated() { return isAuthenticated; }
	public boolean isSupportCompress() { return isSupportCompress; }
	public void setSupportCompress(boolean isSupportCompress) { this.isSupportCompress = isSupportCompress; }
	public void setAutocommit(boolean autocommit) { this.autocommit = autocommit; }
	public String getPassword() { return password; }
	public void setPassword(String password) { this.password = password; }
	public long getLastInsertId() { return lastInsertId; }
	public void setLastInsertId(long lastInsertId) { this.lastInsertId = lastInsertId; }
	public void setAuthenticated(boolean isAuthenticated) { this.isAuthenticated = isAuthenticated; }
	public String getUser() { return user; }
	public int getCharsetIndex() { return charsetIndex; }
	public void setUser(String user) { this.user = user; }
	public String getSchema() { return schema; }
	public String getCharset() { return charset; }
	public byte[] getSeed() { return seed; }
	public boolean isTxInterrupted() { return txInterrupted; }
	public HandshakePacket getHandshake() { return handshake; }
	public void setHandshake(HandshakePacket handshake) { this.handshake = handshake; }

	/* 写一个错误信息 */
	public void writeErrMessage(int errno, String msg) {
		ErrorPacket err = new ErrorPacket();
		err.packetId = 1;
		err.errno = errno;
		err.message = encodeString(msg, charset);
		err.write(this);
	}

	/* 根据字符集编码src字符串 */
	public final static byte[] encodeString(String src, String charset) {
		if (src == null) {
			return null;
		}

		if (charset == null) {
			return src.getBytes();
		}

		try {
			return src.getBytes(charset);
		} catch (UnsupportedEncodingException e) {
			return src.getBytes();
		}
	}

	/* 设置逻辑DB */
	public void setSchema(String newSchema) {
		String curSchema = schema;
		if (curSchema == null) {
			this.schema = newSchema;
			this.oldSchema = newSchema;
		} else {
			this.oldSchema = curSchema;
			this.schema = newSchema;
		}
	}

	/* 设置字符集合 */
	public boolean setCharsetIndex(int ci) {
		String charset = CharsetUtil.getCharset(ci);
		if (charset != null) {
			return setCharset(charset);
		} else {
			return false;
		}
	}

	/* 设置字符集 */
	public boolean setCharset(String charset) {
		
		//修复PHP字符集设置错误, 如： set names 'utf8'
		if ( charset != null ) {			
			charset = charset.replace("'", "");
		}
		
		int ci = CharsetUtil.getIndex(charset);
		if (ci > 0) {
			this.charset = charset.equalsIgnoreCase("utf8mb4") ? "utf8"
					: charset;
			this.charsetIndex = ci;
			return true;
		} else {
			return false;
		}
	}

	/* 设置是否需要中断当前事务 */
	public void setTxInterrupt(String txInterrputMsg) {
		if (!autocommit && !txInterrupted) {
			txInterrupted = true;
			this.txInterrputMsg = txInterrputMsg;
		}
	}


	private static byte[] passwd(String pass, HandshakePacket hs) throws NoSuchAlgorithmException {
		if (pass == null || pass.length() == 0) {
			return null;
		}
		byte[] passwd = pass.getBytes();
		int sl1 = hs.seed.length;
		int sl2 = hs.restOfScrambleBuff.length;
		byte[] seed = new byte[sl1 + sl2];
		System.arraycopy(hs.seed, 0, seed, 0, sl1);
		System.arraycopy(hs.restOfScrambleBuff, 0, seed, sl1, sl2);
		return SecurityUtil.scramble411(passwd, seed);
	}

	/* 发送认证权限包 */
	public void authenticate() {
		AuthPacket packet = new AuthPacket();
		packet.packetId = 1;
		packet.clientFlags = clientFlags;
		packet.maxPacketSize = maxPacketSize;
		packet.charsetIndex = this.charsetIndex;
		packet.user = user;
		try {
			packet.password = passwd(password, handshake);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e.getMessage());
		}
		packet.database = schema;
		BufferArray bufferArray = NetSystem.getInstance().getBufferPool()
				.allocateArray();
		packet.write(bufferArray);
		// write to connection
		this.write(bufferArray);

	}
}
