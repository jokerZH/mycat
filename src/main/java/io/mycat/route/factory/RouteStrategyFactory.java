package io.mycat.route.factory;

import io.mycat.MycatServer;
import io.mycat.route.RouteStrategy;
import io.mycat.route.impl.DruidMycatRouteStrategy;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/* 路由策略工厂类 */
public class RouteStrategyFactory {
	private static RouteStrategy defaultStrategy = null;
	private static boolean isInit = false;
	private static ConcurrentMap<String,RouteStrategy> strategyMap = new ConcurrentHashMap<String,RouteStrategy>();

	private RouteStrategyFactory() { }
	private static void init() {
		String defaultSqlParser = MycatServer.getInstance().getConfig().getSystem().getDefaultSqlParser();
		defaultSqlParser = defaultSqlParser == null ? "" : defaultSqlParser;

		strategyMap.putIfAbsent("druidparser", new DruidMycatRouteStrategy());
		
		defaultStrategy = strategyMap.get(defaultSqlParser);
		if(defaultStrategy == null) {
			defaultStrategy = strategyMap.get("druidparser");
		}
	}

	public static RouteStrategy getRouteStrategy() {
		if(!isInit) {
			init();
			isInit = true;
		}
		return defaultStrategy;
	}
	
	public static RouteStrategy getRouteStrategy(String parserType) {
		if(!isInit) {
			init();
			isInit = true;
		}
		return strategyMap.get(parserType);
	}
}
