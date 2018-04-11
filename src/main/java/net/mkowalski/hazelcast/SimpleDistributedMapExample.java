package net.mkowalski.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.mkowalski.hazelcast.util.ExampleRunner;
import net.mkowalski.hazelcast.util.MapUtil;
import net.mkowalski.hazelcast.util.WaitUtil;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Example usage of a DistributedMap - every connected node increments its own counter under separate key (random nodeId)
 * within the distributed map. Only because of the separate keys the results are correct.
 */
@Slf4j
public class SimpleDistributedMapExample {

    private static final long EXPECTED_NODES_COUNT = 2;
    private static final long ITERATION_COUNT = 25L;

    public static void main(String[] args) {
        ExampleRunner.run(SimpleDistributedMapExample::runExample, EXPECTED_NODES_COUNT);
    }

    private static void runExample(HazelcastInstance hazelcastInstance) {
        // obtain distributed map handler
        ConcurrentMap<String, Counter> map = hazelcastInstance.getMap("simple-distributed-map");

        final String nodeId = UUID.randomUUID().toString();

        // populate map with data
        map.putIfAbsent(nodeId, new Counter(WaitUtil.nowFormatted(), 0));

        while (map.get(nodeId).getValue() < ITERATION_COUNT) {
            // unsafe!
            map.put(nodeId, new Counter(WaitUtil.nowFormatted(), map.get(nodeId).getValue() + 1));

            MapUtil.logMapState(map, nodeId);
            WaitUtil.sleepMillis(500);
        }

        WaitUtil.sleepMillis(1000);
        MapUtil.logMapState(map, nodeId);
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Counter implements Serializable {
        private String timestamp;
        private int value;
    }

}
