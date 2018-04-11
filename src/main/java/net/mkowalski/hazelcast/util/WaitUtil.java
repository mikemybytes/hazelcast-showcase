package net.mkowalski.hazelcast.util;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import lombok.extern.slf4j.Slf4j;

import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Slf4j
public class WaitUtil {

    private WaitUtil() {
        // hidden
    }

    public static void sleepMillis(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void sleepBetweenMillis(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException();
        }
        int duration = min + new SecureRandom().nextInt(max - min);
        sleepMillis(duration);
    }

    /**
     * Waits until expected number of nodes has connected to the cluster. In a real-life application, there is no need
     * for such hacks - in this case, we just want to make sure that cluster is complete before starting processing
     * (all examples executes quickly). "Waiting" is done using a naive approach (polling).
     */
    public static void waitForAllNodesAvailable(HazelcastInstance hazelcastInstance, long numberOfNodes) {
        IAtomicLong connectedNodesNum = hazelcastInstance.getAtomicLong("connected-nodes-number");
        if (connectedNodesNum.get() == numberOfNodes) {
            throw new IllegalStateException("Adding new nodes is not allowed now");
        }

        log.info("Nodes available: {} of {}", connectedNodesNum.incrementAndGet(), numberOfNodes);

        while (connectedNodesNum.get() != numberOfNodes) {
            log.info("Waiting for other nodes to connect...");
            sleepMillis(1000);
        }
    }

    public static String nowFormatted() {
        return LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
    }

}
