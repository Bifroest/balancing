package com.goodgame.profiling.bifroest.balancing.statistics;

import com.goodgame.profiling.bifroest.balancing.PopulationStatistics;
import com.goodgame.profiling.commons.statistics.WriteToStorageEvent;
import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;
import com.goodgame.profiling.commons.statistics.eventbus.EventBusRegistrationPoint;
import com.goodgame.profiling.commons.statistics.gathering.StatisticGatherer;
import com.goodgame.profiling.commons.statistics.storage.MetricStorage;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import org.kohsuke.MetaInfServices;

@MetaInfServices
public class BucketBalanceTracker implements StatisticGatherer {

    private Collection<Long> bucketSizes;

    @Override
    public void init() {
        EventBusRegistrationPoint reg = EventBusManager.createRegistrationPoint();
        reg.subscribe( NodeJoinFinished.class, e -> bucketSizes = e.getBucketSizes() );
        reg.subscribe( NodeLeaveFinished.class, e -> bucketSizes = e.getBucketSizes() );

        reg.subscribe( WriteToStorageEvent.class, e -> {
            MetricStorage storage = e.storageToWriteTo()
                                     .getSubStorageCalled( "mapping" )
                                     .getSubStorageCalled( "bucket-sizes" );

            if ( bucketSizes != null ) {
                double min = Collections.min( bucketSizes ).doubleValue(); // javac doesn't need to .doubleValue(), but eclipse does :(
                PopulationStatistics.evaluate(
                        bucketSizes.stream()
                                   .map( l -> l / min - 1 )
                                   .collect( Collectors.toList() )
                ).writeTo( storage );
            }
        });
    }
    
}
