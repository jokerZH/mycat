package io.mycat.server.config.node;

import java.util.Properties;

/* 保存主机名和index的对应关系 */
public class HostIndexConfig {
	private Properties props = new Properties();	/* TODO */

	public Properties getProps() { return props; }
	public void setProps(Properties props) { this.props = props; }
}
