package io.bifroest.balancing;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.subquark.mtdt.generators.StringGenerator;

import io.bifroest.commons.statistics.WriteToStorageEvent;
import io.bifroest.commons.statistics.eventbus.EventBus;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.commons.statistics.eventbus.disruptor.DisruptorEventBus;
import io.bifroest.commons.statistics.gathering.CompositeStatisticGatherer;
import io.bifroest.commons.statistics.storage.JSONMetricStorage;

public class BucketMappingTest {

    private static final int MORE_ITERATIONS = 50;
    private static final int ITERATIONS = 1000;

    private final Random rand;
    private final LowLevelBucketMapping<JSONSerializableString> subject;
    private final ControlGroup controlGroup;

    public BucketMappingTest( Random rand, LowLevelBucketMapping<JSONSerializableString> subject, ControlGroup controlGroup ) {
        this.rand = rand;
        this.subject = subject;
        this.controlGroup = controlGroup;
    }

    public void randomlyJoinOrLeaveNode() {
        if ( subject.isLonelyBucket() || rand.nextBoolean()) {
            JSONSerializableString node = createRandomNode( rand );
            if ( controlGroup.getKnownNodes().contains( node ) ) {
                // Avoid duplicates by just doing nothing
                return;
            }
            controlGroup.add( node );
            subject.joinNode( node );
        } else {
            JSONSerializableString node = selectRandomNode();
            controlGroup.remove( node );
            subject.leaveNode( node );
        }
    }

    private static JSONSerializableString createRandomNode( Random rand ) {
        return new JSONSerializableString( "node." + StringGenerator.mostlyReadableStrings( 8 ).generateRandomInput( rand ) );
    }

    private JSONSerializableString selectRandomNode() {
        int nodeIndex = rand.nextInt( controlGroup.getKnownNodes().size() );
        int currentIndex = 0;
        for ( JSONSerializableString node : controlGroup.getKnownNodes() ) {
            if ( nodeIndex == currentIndex ) {
                return node;
            }
            currentIndex++;
        }
        throw new IllegalStateException("what");
    }
    
    private static void runTestWithSubject( Random rand, LowLevelBucketMapping<JSONSerializableString> subject, ControlGroup controlGroup ) throws InterruptedException {
        EventBus bus = new DisruptorEventBus( 1, 8 );
        EventBusManager.setEventBus( bus );
        new CompositeStatisticGatherer().init();
        
        BucketMappingTest test = new BucketMappingTest( rand, subject, controlGroup );
        for (int i = 0; i < ITERATIONS; i++) {
            test.randomlyJoinOrLeaveNode();
        }
        JSONMetricStorage storage = new JSONMetricStorage();
        EventBusManager.synchronousFire( new WriteToStorageEvent(Clock.systemUTC(), storage) );
        System.out.println( storage.storageAsJSON().getJSONObject("mapping").toString(2) );
        EventBusManager.shutdownEventBus();
    }

    public static void main(String[] args) throws InterruptedException {
        Random seeder = new Random();
        for (int j = 0; j < MORE_ITERATIONS; j++) {
            int seed = seeder.nextInt();

            ControlGroup controlGroup;
            JSONSerializableString initialNode = createRandomNode( seeder );

            System.out.println("Test with balancing:");
            controlGroup = new ControlGroup( initialNode );
            LowLevelBucketMapping<JSONSerializableString> subjectWithBalancing = new KeepNeighboursMapping<>( initialNode );
            subjectWithBalancing = new RebalancingBucketMapping<>( subjectWithBalancing, 2.5 );
            subjectWithBalancing = new ValidatingBucketMapping<>( subjectWithBalancing, controlGroup );
            runTestWithSubject( new Random( seed ), subjectWithBalancing, controlGroup );

            System.out.println("Test without balancing:");
            controlGroup = new ControlGroup( initialNode );
            LowLevelBucketMapping<JSONSerializableString> subjectWithoutBalancing = new KeepNeighboursMapping<>( initialNode );
            subjectWithoutBalancing = new ValidatingBucketMapping<>( subjectWithoutBalancing, controlGroup );
            runTestWithSubject( new Random( seed ), subjectWithoutBalancing, controlGroup );
        }
    }

    private static class ControlGroup implements KnownNodesProvider<JSONSerializableString> {

        private final Set<JSONSerializableString> nodes;

        public ControlGroup( JSONSerializableString initialNode ) {
            nodes = new HashSet<>( Arrays.asList( initialNode ) );
        }

        public void add( JSONSerializableString node ) {
            nodes.add( node );
        }

        public void remove( JSONSerializableString node ) {
            nodes.remove( node );
        }

        @Override
        public Collection<JSONSerializableString> getKnownNodes() {
            return nodes;
        }
    }

}
