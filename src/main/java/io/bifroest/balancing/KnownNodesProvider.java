package io.bifroest.balancing;

import java.util.Collection;

public interface KnownNodesProvider<N> {
    Collection<N> getKnownNodes();
}
