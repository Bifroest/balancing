package io.bifroest.balancing;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.json.JSONObject;
import org.junit.Test;

public class KeepNeighboursMappingTest {

    @Test
    public void testThatJoiningCreatesABucket() {
        KeepNeighboursMapping<JSONSerializableString> subject = new KeepNeighboursMapping<>( new JSONSerializableString( "node.initial" ) );
        assertEquals( 1, subject.countBuckets() );
        subject.joinNode( new JSONSerializableString( "node.first" ) );
        assertEquals( 2, subject.countBuckets() );
        subject.joinNode( new JSONSerializableString( "node.second" ) );
        assertEquals( 3, subject.countBuckets() );
    }

    @Test
    public void testThatLeavingRemovesABucket() {
        KeepNeighboursMapping<JSONSerializableString> subject = new KeepNeighboursMapping<>( new JSONSerializableString( "node.initial" ) );
        subject.joinNode( new JSONSerializableString( "node.first" ) );
        subject.leaveNode( new JSONSerializableString( "node.initial" ) );
        assertEquals( 1, subject.countBuckets() );
    }

    @Test
    public void testThatDeserializationWorks() {
        KeepNeighboursMapping<JSONSerializableString> subject = new KeepNeighboursMapping<>( new JSONSerializableString( "node.initial" ) );
        subject.joinNode( new JSONSerializableString( "node.first" ) );

        JSONObject serializedMapping = subject.toJSON();
        KeepNeighboursMapping<JSONSerializableString> deserializedMapping = KeepNeighboursMapping.fromJSON( serializedMapping,
                                                                                                            JSONSerializableString::fromJSON );

        assertThat( subject.countBuckets(), is( deserializedMapping.countBuckets() ) );
        assertThat( subject.getNodeFor( 93 ), is( deserializedMapping.getNodeFor( 93 ) ) );

        subject.forEach( bucket -> {
            Bucket<JSONSerializableString> result = deserializedMapping.findFirst( bucket2 -> bucket.equals( bucket2 ) );
            assertThat( result, is( notNullValue() ) );
        });
    }
}
