package net.mkowalski.hazelcast.util;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class MapUtil {

    private MapUtil() {
        // hidden
    }

    /**
     * Logs map state on the given node.
     */
    public static void logMapState(Map<?, ?> map, String nodeId) {
        log.info("Map state at {} on {}", WaitUtil.nowFormatted(), nodeId);
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            log.info("    {} -> {}", entry.getKey(), entry.getValue());
        }
    }

}
