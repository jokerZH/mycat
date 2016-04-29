package io.mycat.server.config.node;

import java.util.HashMap;
import java.util.Map;

/* 字符集相关 TODO */
public class CharsetConfig {
	private Map<String, Object> props = new HashMap<String, Object>();	/*TODO */

	public Map<String, Object> getProps() { return props; }
	public void setProps(Map<String, Object> props) { this.props = props; }
}
