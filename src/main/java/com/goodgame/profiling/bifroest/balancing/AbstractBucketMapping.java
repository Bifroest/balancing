package com.goodgame.profiling.bifroest.balancing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.goodgame.profiling.bifroest.balancing.statistics.NodeJoinFinished;
import com.goodgame.profiling.bifroest.balancing.statistics.NodeJoinStarted;
import com.goodgame.profiling.commons.serialize.json.JSONSerializable;

public abstract class AbstractBucketMapping<N extends JSONSerializable> implements LowLevelBucketMapping<N> {

    private Bucket<N> head;

    public AbstractBucketMapping( N initialNode ) {
        NodeJoinStarted.fire( initialNode.toString() );
        head = new Bucket<>( initialNode, 12407, 12407, null, null );
        head.setNext( head );
        head.setPrevious( head );
        NodeJoinFinished.fire( initialNode.toString(), bucketSizes() );
    }

    protected AbstractBucketMapping( Bucket<N> head ) {
        this.head = head;
    }

    protected Bucket<N> head() {
        return head;
    }

    @Override
    public boolean isLonelyBucket() {
        return head.next() == head;
    }

    protected void removeBucket( Bucket<N> bucket ) {
        this.head = bucket.unlink();
    }

    protected int countBuckets() {
        int count = 0;
        Bucket<N> current = head;
        do {
            count++;
            current = current.next();
        } while ( current != head );
        return count;
    }

    protected Collection<Long> bucketSizes() {
        Collection<Long> result = new ArrayList<>();
        forEach( bucket -> result.add(bucket.size() ) );
        return result;
    }

    @Override
    public N getNodeFor( int bucketIndex ) {
        return findFirst( bucket -> bucket.contains( bucketIndex ) ).node();
    }

    @Override
    public Bucket<N> getBucketFor( N node ) {
        return findFirst( bucket -> bucket.node().equals( node ) );
    }

    protected Stream<Bucket<N>> buckets() {
        Stream.Builder<Bucket<N>> builder = Stream.builder();
        Bucket<N> current = head;
        do {
            builder.add( current );
            current = current.next();
        } while ( current != head );
        return builder.build();
    }
    
    @Override
    public double getWorstSizeImbalance() {
        Collection<Long> sizes = bucketSizes();
        long min = Collections.min( sizes );
        long max = Collections.max( sizes );
        return max / min;
    }

    @Override
    public void forEach( Consumer<Bucket<N>> consumer ) {
        Bucket<N> current = head;
        do {
            consumer.accept( current );
            current = current.next();
        } while ( current != head );
    }

    @Override
    public Bucket<N> findFirst( Predicate<Bucket<N>> predicate ) {
        Bucket<N> current = head;
        do {
            if ( predicate.test( current ) ) {
                return current;
            }
            current = current.next();
        } while ( current != head );
        return null;
    }

    @Override
    public Bucket<N> max( Comparator<Bucket<N>> comp ) {
        Bucket<N> largest = head;
        Bucket<N> current = head;
        do {
            if ( comp.compare( current, largest ) > 0 ) {
                largest = current;
            }
            current = current.next();
        } while ( current != head );
        return largest;
    }

    @Override
    public Bucket<N> min( Comparator<Bucket<N>> comp ) {
        Bucket<N> smallest = head;
        Bucket<N> current = head;
        do {
            if ( comp.compare( current, smallest ) < 0 ) {
                smallest = current;
            }
            current = current.next();
        } while ( current != head );
        return smallest;
    }
}
