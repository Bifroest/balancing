package io.bifroest.balancing.statistics;


import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import org.kohsuke.MetaInfServices;

import io.bifroest.balancing.PopulationStatistics;
import io.bifroest.commons.statistics.WriteToStorageEvent;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.commons.statistics.eventbus.EventBusRegistrationPoint;
import io.bifroest.commons.statistics.gathering.StatisticGatherer;
import io.bifroest.commons.statistics.storage.MetricStorage;

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
