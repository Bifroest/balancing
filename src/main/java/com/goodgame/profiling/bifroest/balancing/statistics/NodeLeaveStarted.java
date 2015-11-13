package com.goodgame.profiling.bifroest.balancing.statistics;

import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;
import java.time.Instant;

public final class NodeLeaveStarted {

    private final Instant when;
    private final String node;

    private NodeLeaveStarted( Instant when, String node ) {
        this.when = when;
        this.node = node;
    }

    public Instant getWhen() {
        return when;
    }

    public String getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "NodeLeaveStarted{" + "when=" + when + ", node=" + node + '}';
    }

    public static void fire( String node ) {
        EventBusManager.fire(new NodeLeaveStarted( Instant.now(), node ) );
    }

}
