package com.goodgame.profiling.bifroest.balancing;

import java.util.Comparator;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.goodgame.profiling.commons.serialize.json.JSONSerializable;

public interface LowLevelBucketMapping<N extends JSONSerializable> extends BucketMapping<N> {
    boolean isLonelyBucket();
    double getWorstSizeImbalance();
    Bucket<N> min( Comparator<Bucket<N>> comparator );
    Bucket<N> max( Comparator<Bucket<N>> comparator );
    Bucket<N> findFirst( Predicate<Bucket<N>> predicate );
    void forEach( Consumer<Bucket<N>> consumer );
}
