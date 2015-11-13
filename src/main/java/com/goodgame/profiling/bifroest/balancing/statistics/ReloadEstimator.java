package com.goodgame.profiling.bifroest.balancing.statistics;

import com.goodgame.profiling.commons.statistics.WriteToStorageEvent;
import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;
import com.goodgame.profiling.commons.statistics.eventbus.EventBusRegistrationPoint;
import com.goodgame.profiling.commons.statistics.gathering.StatisticGatherer;
import com.goodgame.profiling.commons.statistics.storage.MetricStorage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.MetaInfServices;

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
