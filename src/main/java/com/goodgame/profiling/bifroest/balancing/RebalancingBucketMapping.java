package com.goodgame.profiling.bifroest.balancing;

import java.util.Comparator;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.goodgame.profiling.commons.serialize.json.JSONSerializable;

import org.json.JSONObject;

public class RebalancingBucketMapping<N extends JSONSerializable> implements LowLevelBucketMapping<N> {

    private final LowLevelBucketMapping<N> inner;
    private final double threshold;

    public RebalancingBucketMapping( LowLevelBucketMapping<N> inner, double threshold ) {
        this.inner = inner;
        this.threshold = threshold;
    }

    @Override
    public double getWorstSizeImbalance() {
        return inner.getWorstSizeImbalance();
    }

    @Override
    public void joinNode( N node ) {
        inner.joinNode( node );
    }

    @Override
    public void leaveNode( N node ) {
        inner.leaveNode( node );
        if ( inner.getWorstSizeImbalance() > threshold ) {
            N smallestNode = inner.min( (a, b) -> Long.compare( a.size(), b.size() ) ).node();
            inner.leaveNode( smallestNode );
            inner.joinNode( smallestNode );
        }
    }

    @Override
    public N getNodeFor( int bucketIndex ) {
        return inner.getNodeFor( bucketIndex );
    }

    @Override
    public Bucket<N> getBucketFor( N node ) {
        return inner.getBucketFor( node );
    }

    @Override
    public Bucket<N> min(Comparator<Bucket<N>> comparator) {
        return inner.min(comparator);
    }

    @Override
    public Bucket<N> max(Comparator<Bucket<N>> comparator) {
        return inner.max(comparator);
    }

    @Override
    public Bucket<N> findFirst(Predicate<Bucket<N>> predicate) {
        return inner.findFirst(predicate);
    }

    @Override
    public void forEach(Consumer<Bucket<N>> consumer) {
        inner.forEach(consumer);
    }

    @Override
    public boolean isLonelyBucket() {
        return inner.isLonelyBucket();
    }

    @Override
    public JSONObject toJSON() {
        return inner.toJSON();
    }
    
}
