package net.mkowalski.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import lombok.extern.slf4j.Slf4j;
import net.mkowalski.hazelcast.util.ExampleRunner;
import net.mkowalski.hazelcast.util.InputDataLibrary;
import net.mkowalski.hazelcast.util.MapUtil;
import net.mkowalski.hazelcast.util.WaitUtil;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Extended version of the {@link DistributedMapWithoutLockExample}. In this case, to prevent from concurrent
 * (non-atomic) modifications, single global distributed lock is used. In terms of concurrency throughput,
 * {@link DistributedMapWithLockExample} should perform better than this approach. This is just an example
 * of implementing distributed critical section using Hazelcast locks.
 */
@Slf4j
public class DistributedMapWithGlobalLockExample {

    private static final long EXPECTED_NODES_COUNT = 3;
    private static final int ITERATION_COUNT = 2000;

    public static void main(String[] args) {
        ExampleRunner.run(DistributedMapWithGlobalLockExample::example, EXPECTED_NODES_COUNT);
    }

    private static void example(HazelcastInstance hazelcastInstance) {
        ConcurrentMap<String, Long> map = hazelcastInstance.getMap("distributed-map-with-lock");

        final String nodeId = UUID.randomUUID().toString();

        // populate map with data
        InputDataLibrary.colors().forEach(color -> map.putIfAbsent(color, 0L));

        final Queue<String> colors = InputDataLibrary.colorsRepeated(ITERATION_COUNT);

        while (!colors.isEmpty()) {
            final String color = colors.remove();

            // distributed lock!
            ILock lock = hazelcastInstance.getLock("colors-lock");
            lock.lock();
            try {
                // now we are executing our code exclusively
                map.put(color, map.get(color) + 1);
            } finally {
                lock.unlock();
            }

            MapUtil.logMapState(map, nodeId);
        }

        WaitUtil.sleepMillis(1000);
        MapUtil.logMapState(map, nodeId);
    }

}
