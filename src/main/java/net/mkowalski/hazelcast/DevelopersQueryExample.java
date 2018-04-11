package net.mkowalski.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SqlPredicate;
import lombok.extern.slf4j.Slf4j;
import net.mkowalski.hazelcast.util.ExampleRunner;
import net.mkowalski.hazelcast.util.InputDataLibrary;
import net.mkowalski.hazelcast.util.WaitUtil;

import java.util.Collection;
import java.util.UUID;

/**
 * Example implementation of the distributed queries using Hazelcast. Instead of collecting all data on one node
 * (which may require a lot of memory), Hazelcast executes it in a distributed way and than aggregates the results.
 */
@Slf4j
public class DevelopersQueryExample {

    private static final long EXPECTED_NODES_COUNT = 3;
    private static final int NODE_DEVELOPERS_COUNT = 500;

    public static void main(String[] args) {
        ExampleRunner.run(DevelopersQueryExample::example);
    }

    private static void example(HazelcastInstance hazelcastInstance) {
        IMap<String, Developer> map = hazelcastInstance.getMap("developer-query-map");

        IAtomicLong connectedNodesNum = hazelcastInstance.getAtomicLong("connected-nodes-number");
        if (connectedNodesNum.get() == EXPECTED_NODES_COUNT) {
            throw new IllegalStateException("Adding new nodes is not allowed now");
        }

        for (int i = 0; i < NODE_DEVELOPERS_COUNT; ++i) {
            // add random developers to the distributed map
            map.put(UUID.randomUUID().toString(), InputDataLibrary.randomDeveloper());
        }

        WaitUtil.waitForAllNodesAvailable(hazelcastInstance, EXPECTED_NODES_COUNT);

        WaitUtil.sleepMillis(1000);
        log.info("Developers stored locally: {}", map.localKeySet().size());

        EntryObject dev = new PredicateBuilder().getEntryObject();
        Predicate predicate = dev.isNot("male")
                .and(dev.get("age").lessThan(30))
                .and(dev.get("salary").greaterThan(10000));


        Collection<Developer> youngAndRichFemales = map.values(predicate);
        log.info("Young and rich female developer females ({}): {}", youngAndRichFemales.size(), youngAndRichFemales);

        SqlPredicate sqlPredicate = new SqlPredicate("male AND age > 50 AND salary < 5000");
        Collection<Developer> olderNotSoRichMales = map.values(sqlPredicate);
        log.info("Older and not so rich males ({}): {}", olderNotSoRichMales.size(), olderNotSoRichMales);
    }

}
