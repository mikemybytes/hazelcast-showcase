package net.mkowalski.hazelcast.util;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

@Slf4j
public class ExampleRunner {

    private ExampleRunner() {
        // static only
    }

    /**
     * Runs provided example and then closes Hazelcast gracefully.
     */
    public static void run(Consumer<HazelcastInstance> example) {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(new Config());
        try {
            example.accept(hazelcastInstance);
        } finally {
            closeGracefully(hazelcastInstance);
        }
    }

    /**
     * Waits until all expected nodes are connected, then runs provided example and closes Hazelcast gracefully.
     */
    public static void run(Consumer<HazelcastInstance> example, long expectedNodesCount) {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(new Config());
        try {
            WaitUtil.waitForAllNodesAvailable(hazelcastInstance, expectedNodesCount);
            example.accept(hazelcastInstance);
        } finally {
            closeGracefully(hazelcastInstance);
        }
    }

    private static void closeGracefully(HazelcastInstance hazelcastInstance) {
        // wait until other nodes finished too
        WaitUtil.sleepBetweenMillis(1000, 3000);

        log.info("Processing finished - shutting down...");
        hazelcastInstance.shutdown();
    }

}
