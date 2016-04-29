package io.mycat.route.function;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 路由分片函数抽象类
 * 为了实现一个默认的支持范围分片的函数 calcualteRange
 * 重写它以实现自己的范围路由规则
 */
public abstract class AbstractPartitionAlgorithm implements RuleAlgorithm {
	private Map<String, Object> config = new LinkedHashMap<String, Object>();	/*  参数*/

	@Override
	public void init() { }

	/**
	 * 返回所有被路由到的节点的编号
	 * 返回长度为0的数组表示所有节点都被路由（默认）
	 * 返回null表示没有节点被路由到
	 */
	@Override
	public Integer[] calculateRange(String beginValue, String endValue) {
		return new Integer[0];
	}

	/* 分表计算beging和end对应的下标,然后返回数组,包含两个下标中的所有数字, 对sharding算法有特殊要求 */
	public static Integer[] calculateSequenceRange(AbstractPartitionAlgorithm algorithm, String beginValue, String endValue) {
		Integer begin = 0, end = 0;
		begin = algorithm.calculate(beginValue);
		end = algorithm.calculate(endValue);

		if(begin == null || end == null){
			return new Integer[0];
		}

		if (end >= begin) {
			int len = end-begin+1;
			Integer [] re = new Integer[len];

			for(int i =0;i<len;i++){
				re[i]=begin+i;
			}

			return re;
		}else{
			return null;
		}
	}

	public Map<String, Object> getConfig() { return config; }
	public void setConfig(Map<String, Object> config) { this.config = config; }
}
