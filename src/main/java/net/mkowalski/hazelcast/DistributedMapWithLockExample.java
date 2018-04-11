package net.mkowalski.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import lombok.extern.slf4j.Slf4j;
import net.mkowalski.hazelcast.util.ExampleRunner;
import net.mkowalski.hazelcast.util.InputDataLibrary;
import net.mkowalski.hazelcast.util.MapUtil;
import net.mkowalski.hazelcast.util.WaitUtil;

import java.util.Queue;
import java.util.UUID;

/**
 * Extended version of the {@link DistributedMapWithoutLockExample}. In this case, to prevent from concurrent
 * (non-atomic) modifications, single key locking feature of {@link IMap} is used.
 */
@Slf4j
public class DistributedMapWithLockExample {

    private static final long EXPECTED_NODES_COUNT = 3;
    private static final int ITERATION_COUNT = 2000;

    public static void main(String[] args) {
        ExampleRunner.run(DistributedMapWithLockExample::example, EXPECTED_NODES_COUNT);
    }

    private static void example(HazelcastInstance hazelcastInstance) {
        IMap<String, Long> map = hazelcastInstance.getMap("distributed-map-without-lock");

        final String nodeId = UUID.randomUUID().toString();

        // populate map with data
        InputDataLibrary.colors().forEach(color -> map.putIfAbsent(color, 0L));

        final Queue<String> colors = InputDataLibrary.colorsRepeated(ITERATION_COUNT);

        while (!colors.isEmpty()) {
            final String color = colors.remove();
            // lock single key - other keys can still be updated in the meantime!
            map.lock(color);
            try {
                Long currentCounter = map.get(color);
                map.put(color, currentCounter + 1);
            } finally {
                map.unlock(color);
            }

            MapUtil.logMapState(map, nodeId);
        }

        WaitUtil.sleepMillis(1000);
        MapUtil.logMapState(map, nodeId);
    }

}
