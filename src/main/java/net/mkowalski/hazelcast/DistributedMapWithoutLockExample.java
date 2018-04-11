package net.mkowalski.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import lombok.extern.slf4j.Slf4j;
import net.mkowalski.hazelcast.util.ExampleRunner;
import net.mkowalski.hazelcast.util.InputDataLibrary;
import net.mkowalski.hazelcast.util.MapUtil;
import net.mkowalski.hazelcast.util.WaitUtil;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * A bit more sophisticated example usage of a DistributedMap - each node has its own local list of repeated color
 * names. Then nodes increments each color counter (map entry) for every occurrence. Hazelcast's DistributedMap
 * implements {@link ConcurrentMap} interface, so it allows concurrent modifications but does not guarantee that
 * updates are atomic (the code first reads the current value and then puts incremented counter back to the map).
 * For multiple nodes, the result won't be correct.
 */
@Slf4j
public class DistributedMapWithoutLockExample {

    private static final long EXPECTED_NODES_COUNT = 3;
    private static final int ITERATION_COUNT = 2000;

    public static void main(String[] args) {
        ExampleRunner.run(DistributedMapWithoutLockExample::example, EXPECTED_NODES_COUNT);
    }

    private static void example(HazelcastInstance hazelcastInstance) {
        ConcurrentMap<String, Long> map = hazelcastInstance.getMap("distributed-map-without-lock");

        final String nodeId = UUID.randomUUID().toString();

        // populate map with data
        InputDataLibrary.colors().forEach(color -> map.putIfAbsent(color, 0L));

        final Queue<String> colors = InputDataLibrary.colorsRepeated(ITERATION_COUNT);

        while (!colors.isEmpty()) {
            final String color = colors.remove();
            final Long currentCounter = map.get(color);
            map.put(color, currentCounter + 1);

            MapUtil.logMapState(map, nodeId);
        }

        WaitUtil.sleepMillis(1000);
        MapUtil.logMapState(map, nodeId);
    }

}
