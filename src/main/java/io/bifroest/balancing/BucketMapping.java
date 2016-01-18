package io.bifroest.balancing;

import io.bifroest.commons.serialize.json.JSONSerializable;

public interface BucketMapping<N extends JSONSerializable> extends JSONSerializable {
    void joinNode( N node );
    void leaveNode( N node );
    N getNodeFor( int bucketIndex );
    Bucket<N> getBucketFor( N node );
}
