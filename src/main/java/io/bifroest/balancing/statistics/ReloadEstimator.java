package io.bifroest.balancing.statistics;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.MetaInfServices;

import io.bifroest.commons.statistics.WriteToStorageEvent;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.commons.statistics.eventbus.EventBusRegistrationPoint;
import io.bifroest.commons.statistics.gathering.StatisticGatherer;
import io.bifroest.commons.statistics.storage.MetricStorage;

@MetaInfServices
public class ReloadEstimator implements StatisticGatherer {
    private static final Logger log = LogManager.getLogger();
    
    private long totalIncreases = 0;
    private long increases = 0;

    @Override
    public void init() {
        EventBusRegistrationPoint reg = EventBusManager.createRegistrationPoint();
        reg.subscribe( NodeJoinStarted.class, e -> increases = 0 );
        reg.subscribe( NodeJoinFinished.class, e -> {
            log.debug( "Join of {} reloaded {} things", e.getNode(), increases );
            increases = Long.MIN_VALUE;
        });

        reg.subscribe( NodeLeaveStarted.class, e -> increases = 0 );
        reg.subscribe( NodeLeaveFinished.class, e -> {
            log.debug( "Leave of {} reloaded {} things", e.getNode(), increases );
            increases = Long.MIN_VALUE;
        });

        reg.subscribe( BucketIncreasedEvent.class, e -> {
            totalIncreases += e.getIncrease();
            increases += e.getIncrease();
        });

        reg.subscribe( WriteToStorageEvent.class, e -> {
            MetricStorage storage = e.storageToWriteTo().getSubStorageCalled( "mapping" );
            storage.store( "estimated-reloads", totalIncreases );
        });
    }

}
