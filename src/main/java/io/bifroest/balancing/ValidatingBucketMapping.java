package io.bifroest.balancing;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.json.JSONObject;

import io.bifroest.commons.serialize.json.JSONSerializable;

public class ValidatingBucketMapping<N extends JSONSerializable> implements LowLevelBucketMapping<N> {

    private final LowLevelBucketMapping<N> inner;
    private final KnownNodesProvider<N> knownNodesProvider;

    public ValidatingBucketMapping( LowLevelBucketMapping<N> inner, KnownNodesProvider<N> knownNodesProvider ) {
        this.inner = inner;
        this.knownNodesProvider = knownNodesProvider;
    }

    private void validateBucketMapping() {
        // Check that all buckets are right next to each other
        forEach( bucket -> {
            if ( bucket.end() != bucket.next().start() ) {
                throw new IllegalStateException( "Bucket borders do not match: " + bucket + " <-> " + bucket.next() );
            }
        });

        // Check that the complete size of the matches the number of integers
        LongAdder size = new LongAdder();
        forEach( bucket -> size.add( bucket.size() ) );
        if ( size.longValue() != 2l<<31 ) {
            throw new IllegalStateException( "Total size of bucket mapping is too large: " + size.longValue() );
        }

        // Check that every bucket node is known
        Collection<N> knownNodes = knownNodesProvider.getKnownNodes();
        forEach( bucket -> {
            if ( !knownNodes.contains( bucket.node() ) ) {
                throw new IllegalStateException( "Mapping for unknow node exist: " + bucket.node() );
            }
        });

        // Check that every known node gets a bucket
        Set<N> copyOfKnownNodes = new HashSet<>( knownNodes );
        forEach( bucket -> copyOfKnownNodes.remove( bucket.node() ) );
        if ( !copyOfKnownNodes.isEmpty() ) {
            throw new IllegalStateException( "Some nodes are not included in the bucket mapping: " + copyOfKnownNodes );
        }
    }

    @Override
    public boolean isLonelyBucket() {
        return inner.isLonelyBucket();
    }

    @Override
    public double getWorstSizeImbalance() {
        return inner.getWorstSizeImbalance();
    }

    @Override
    public Bucket<N> min( Comparator<Bucket<N>> comparator ) {
        return inner.max( comparator );
    }

    @Override
    public Bucket<N> max( Comparator<Bucket<N>> comparator ) {
        return inner.max( comparator );
    }

    @Override
    public Bucket<N> findFirst( Predicate<Bucket<N>> predicate ) {
        return inner.findFirst( predicate );
    }

    @Override
    public void forEach( Consumer<Bucket<N>> consumer ) {
        inner.forEach( consumer );
    }

    @Override
    public void joinNode( N node ) {
        inner.joinNode( node );
        validateBucketMapping();
    }

    @Override
    public void leaveNode( N node ) {
        inner.leaveNode( node );
        validateBucketMapping();
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
    public JSONObject toJSON() {
        return inner.toJSON();
    }

}
