package io.bifroest.balancing;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import io.bifroest.balancing.statistics.NodeJoinFinished;
import io.bifroest.balancing.statistics.NodeJoinStarted;
import io.bifroest.balancing.statistics.NodeLeaveFinished;
import io.bifroest.balancing.statistics.NodeLeaveStarted;
import io.bifroest.commons.serialize.json.JSONSerializable;
import io.bifroest.commons.serialize.json.Serializers;

public final class KeepNeighboursMapping<N extends JSONSerializable> extends AbstractBucketMapping<N> {
    private static final Logger log = LogManager.getLogger();

    public KeepNeighboursMapping( N initialNode ) {
        super( initialNode );
        log.info( "Initial {}", initialNode );
    }

    private KeepNeighboursMapping( Bucket<N> head ) {
        super( head );
    }

    @Override
    public void joinNode( N node ) {
        NodeJoinStarted.fire( node.toString() );
        log.info( "Joining {}", node);

        if ( isLonelyBucket() ) {
            log.trace( "Joining lonely bucket {}", head() );
            splitBucket( head(), node );
        } else {
            Bucket<N> largestSingle = max( (a, b) -> Long.compare( a.size(), b.size() ) );
            Bucket<N> largestLeftNeighbour = max( (a, b) -> Long.compare( a.size() + a.next().size(),
                                                                          b.size() + b.next().size() ) );

            long singleSize = largestSingle.size();
            long neighbourSize = largestLeftNeighbour.size() + largestLeftNeighbour.next().size();
            log.trace( "Comparing single size {} with neighbour size {}", singleSize, neighbourSize );

            if ( singleSize > neighbourSize * 2 / 3 ) {
                splitBucket( largestSingle, node );
            } else {
                splitNeighbouringBuckets( largestLeftNeighbour, largestLeftNeighbour.next(), node );
            }
        }

        NodeJoinFinished.fire( node.toString(), bucketSizes() );
    }

    private void splitBucket( Bucket<N> bucket, N newNode ) {
        log.entry( bucket, newNode );
        int newEnd = bucket.end();
        bucket.resize( bucket.start(), bucket.start() + sizeToInt( bucket.size() / 2 ) );
        bucket.append( newNode, newEnd );
    }

    private void splitNeighbouringBuckets( Bucket<N> leftNeighbour, Bucket<N> rightNeighbour, N newNode ) {
        log.entry( leftNeighbour, rightNeighbour, newNode );
        long totalSize = leftNeighbour.size() + rightNeighbour.size();
        int leftBorder = leftNeighbour.start() + sizeToInt( totalSize / 3 );
        int rightBorder = rightNeighbour.end() - sizeToInt( totalSize / 3 );

        leftNeighbour.resize( leftNeighbour.start(), leftBorder );
        rightNeighbour.resize( rightBorder, rightNeighbour.end() );
        leftNeighbour.append( newNode, rightBorder );
    }

    @Override
    public void leaveNode( N node ) {
        NodeLeaveStarted.fire( node.toString() );
        log.info( "Leaving {}", node );

        if ( isLonelyBucket() ) {
            throw new IllegalStateException("You can check out any time you like, but you can never leave!");
        }
        Bucket<N> bucketToRemove = findFirst( bucket -> bucket.node().equals( node ) );
        Bucket<N> leftNeighbour = bucketToRemove.previous();
        Bucket<N> rightNeighbour = bucketToRemove.next();

        if ( leftNeighbour == rightNeighbour ) {
            // If only two buckets are left
            leftNeighbour.resize( leftNeighbour.start(), rightNeighbour.start() );
            removeBucket( bucketToRemove );

        } else {
            long totalSize = leftNeighbour.size() + bucketToRemove.size() + rightNeighbour.size();
            int border = leftNeighbour.start() + sizeToInt( totalSize / 2 );

            leftNeighbour.resize( leftNeighbour.start(), border );
            rightNeighbour.resize( border, rightNeighbour.end() );
            removeBucket( bucketToRemove );
        }

        NodeLeaveFinished.fire( node.toString(), bucketSizes() );
    }

    private int sizeToInt( long size ) {
        if ( size == Integer.MAX_VALUE + 1l ) {
            // The initial bucket has size 1l<<32, causing to be called with Integer.MAX_VALUE + 1l.
            // We can return Integer.MIN_VALUE here, because adding Integer.MAX_VALUE + 1l IS
            // Integer.MIN_VALUE in the RING of integers - and we only use this function
            // in a ring sense.
            return Integer.MIN_VALUE;
        } else if ( size > Integer.MAX_VALUE || size < 0 ) {
            log.error( "Size of {} is not an integer!", size );
            throw new IllegalStateException("Cluster mapping is borked");
        }
        return (int) size;
    }

    @Override
    public JSONObject toJSON() {
        JSONObject result = new JSONObject();
        result.put("buckets", buckets().collect( Serializers.toSerializedJSONArray() ) );
        return result;
    }

    public static <N extends JSONSerializable> KeepNeighboursMapping<N> fromJSON( JSONObject input, Function<JSONObject, N> idFromJSON ) {
        JSONArray jsonBuckets = input.getJSONArray( "buckets" );
        List<Bucket<N>> buckets = new ArrayList<>();

        for( int i = 0; i < jsonBuckets.length(); i++ ) {
            buckets.add( Bucket.fromJSON( jsonBuckets.getJSONObject( i ), idFromJSON ) );
        }

        for(  int i = 0; i < buckets.size(); i++ ) {
            int previous = ( i == 0 ? buckets.size()-1 : i - 1 );
            int next = ( i == buckets.size() - 1 ? 0 : i + 1 );

            Bucket<N> current = buckets.get( i );
            current.setPrevious( buckets.get( previous ) );
            current.setNext( buckets.get( next ) );
        }

        return new KeepNeighboursMapping<>( buckets.get( 0 ) );
    }
}
