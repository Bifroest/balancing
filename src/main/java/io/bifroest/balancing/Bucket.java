package io.bifroest.balancing;

import java.util.Objects;
import java.util.function.Function;

import org.json.JSONObject;

import io.bifroest.balancing.statistics.BucketIncreasedEvent;
import io.bifroest.commons.serialize.json.JSONSerializable;

public final class Bucket<N extends JSONSerializable> implements JSONSerializable {

    private final N node;

    private int start;
    private int end;

    private Bucket<N> previous;
    private Bucket<N> next;

    Bucket( N node, int start, int end, Bucket<N> previous, Bucket<N> next ) {
        this.start = start;
        this.end = end;
        this.node = node;
        this.previous = previous;
        this.next = next;
        
        BucketIncreasedEvent.fire( this.size(), node.toString() );
    }

    public N node() {
        return node;
    }

    public int start() {
        return start;
    }

    public int end() {
        return end;
    }

    public long size() {
        if ( start < end ) {
            return end - (long) start;
        } else {
            return ( 2l * Integer.MAX_VALUE + 2 ) - ( start - (long) end );
        }
    }

    public boolean contains( int value ) {
        if ( start < end ) {
            return ( value >= start ) && ( value < end );
        } else {
            return ( value >= start ) || ( value < end );
        }
    }

    public Bucket<N> previous() {
        return previous;
    }

    public Bucket<N> next() {
        return next;
    }

    public void resize( int start, int end ) {
        long oldSize = this.size();

        this.start = start;
        this.end = end;

        long newSize = this.size();
        if ( newSize > oldSize) {
            BucketIncreasedEvent.fire( newSize - oldSize, node().toString() );
        }
    }

    void setPrevious( Bucket<N> previous ) {
        this.previous = previous;
    }

    void setNext( Bucket<N> next ) {
        this.next = next;
    }

    public Bucket<N> prepend( N node, int start ) {
        Bucket<N> predecessor = new Bucket<>( node, start, this.start(), this.previous(), this );
        this.previous().setNext( predecessor );
        this.setPrevious( predecessor );
        return this;
    }

    public Bucket<N> append( N node, int end ) {
        Bucket<N> successor = new Bucket<>( node, this.end(), end, this, this.next() );
        this.next().setPrevious( successor );
        this.setNext( successor );
        return this;
    }

    public Bucket<N> unlink() {
        Bucket<N> prev = this.previous();
        this.previous().setNext( this.next() );
        this.next().setPrevious( this.previous() );
        this.setNext( null );
        this.setPrevious( null );
        return prev;
    }

    @Override
    public String toString() {
        return String.format( "Bucket{node=%s, start=%12d, end=%12d, size=%12d}", node, start, end, size() );
    }

    @Override
    public JSONObject toJSON(){
        JSONObject result = new JSONObject();
        result.put( "node",  node.toJSON() );
        result.put( "start", start );
        result.put( "end",   end );
        return result;
    }

    public static <N extends JSONSerializable> Bucket<N> fromJSON( JSONObject input, Function<JSONObject, N> idFromJSON ) {
        N node = idFromJSON.apply(input.getJSONObject( "node" ) );
        int start = input.getInt( "start" );
        int end = input.getInt( "end" );
        return new Bucket<>( node, start, end, null, null);
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + Objects.hashCode( this.node );
        hash = 47 * hash + this.start;
        hash = 47 * hash + this.end;
        return hash;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( obj == null ) {
            return false;
        }
        if ( getClass() != obj.getClass() ) {
            return false;
        }
        final Bucket<?> other = (Bucket<?>) obj;
        if ( !Objects.equals( this.node, other.node ) ) {
            return false;
        }
        if ( this.start != other.start ) {
            return false;
        }
        if ( this.end != other.end ) {
            return false;
        }
        return true;
    }
}
