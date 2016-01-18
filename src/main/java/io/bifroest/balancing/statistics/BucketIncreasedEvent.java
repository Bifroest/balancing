package io.bifroest.balancing.statistics;

import io.bifroest.commons.statistics.eventbus.EventBusManager;

public final class BucketIncreasedEvent {

    private final long increase;
    private final String node;

    private BucketIncreasedEvent( long increase, String node ) {
        this.increase = increase;
        this.node = node;
    }

    public long getIncrease() {
        return increase;
    }

    public String getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "BucketIncreasedEvent{" + "increase=" + increase + ", node=" + node + '}';
    }
    
    public static void fire( long increase, String node ) {
        EventBusManager.fire( new BucketIncreasedEvent( increase, node ) );
    }

}
