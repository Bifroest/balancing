/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.goodgame.profiling.bifroest.balancing;

import com.goodgame.profiling.commons.serialize.json.JSONSerializable;

import org.json.JSONObject;


public class JSONSerializableString implements JSONSerializable {
    private final String value;
    
    public JSONSerializableString( String inner ) {
        this.value = inner;
    }

    @Override
    public JSONObject toJSON() {
        return new JSONObject().put( "narv", value );
    }

    public static JSONSerializableString fromJSON( JSONObject input ) {
        return new JSONSerializableString( input.getString( "narv" ) );
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals( Object obj ) {
        if ( obj == null ) {
            return false;
        }
        if ( getClass() != obj.getClass() ) {
            return false;
        }
        final JSONSerializableString other = (JSONSerializableString) obj;
        return value.equals( other.value );
    }


    @Override
    public String toString() {
        return "JSONSerializableString{" + "value=" + value + '}';
    }
}
