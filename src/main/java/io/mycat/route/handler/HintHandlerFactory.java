package io.mycat.route.handler;

import java.util.HashMap;
import java.util.Map;

/* sql注释的类型处理handler 集合，现在支持两种类型的处理：sql,schema */
public class HintHandlerFactory {
	private static boolean isInit = false;  /* 是否初始化 */
    private static Map<String/*type*/,HintHandler> hintHandlerMap = new HashMap<String,HintHandler>();  /* type和handler的映射关系 */

    private HintHandlerFactory() {}
    private static void init() {
        hintHandlerMap.put("sql",new HintSQLHandler());
        hintHandlerMap.put("schema",new HintSchemaHandler());
        hintHandlerMap.put("datanode",new HintDataNodeHandler());
        hintHandlerMap.put("catlet",new HintCatletHandler());
    }
    
    public static HintHandler getHintHandler(String hintType) {
    	if(!isInit) {
            init();
    	}
    	return hintHandlerMap.get(hintType);
    }
}
