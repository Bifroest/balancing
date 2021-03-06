package io.bifroest.balancing.statistics;

import java.time.Instant;
import java.util.Collection;

import io.bifroest.commons.statistics.eventbus.EventBusManager;

public final class NodeJoinFinished {

    private final Instant when;
    private final String node;
    private final Collection<Long> bucketSizes;

    private NodeJoinFinished( Instant when, String node, Collection<Long> bucketSizes ) {
        this.when = when;
        this.node = node;
        this.bucketSizes = bucketSizes;
    }

    public Instant getWhen() {
        return when;
    }

    public String getNode() {
        return node;
    }

    public Collection<Long> getBucketSizes() {
        return bucketSizes;
    }

    @Override
    public String toString() {
        return "NodeJoinFinished{" + "when=" + when + ", node=" + node + ", bucketSizes=" + bucketSizes + '}';
    }

    public static void fire( String node, Collection<Long> bucketSizes ) {
        EventBusManager.fire(new NodeJoinFinished( Instant.now(), node, bucketSizes ) );
    }

}
