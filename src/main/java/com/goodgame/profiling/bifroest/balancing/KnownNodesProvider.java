package com.goodgame.profiling.bifroest.balancing;

import java.util.Collection;

public interface KnownNodesProvider<N> {
    Collection<N> getKnownNodes();
}
