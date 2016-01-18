package io.bifroest.balancing;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import io.bifroest.commons.statistics.storage.MetricStorage;

public class PopulationStatistics {

    private final long size;
    private final double min;
    private final double max;
    private final double avg;
    private final double stdDeviation;

    private PopulationStatistics( long size, double min, double max, double avg, double stdDeviation ) {
        this.size = size;
        this.min = min;
        this.max = max;
        this.avg = avg;
        this.stdDeviation = stdDeviation;
    }

    public long getSize() {
        return size;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getAvg() {
        return avg;
    }

    public double getStandardDeviation() {
        return stdDeviation;
    }

    @Override
    public String toString() {
        return String.format( "PopulationStatistics{size=%d, min=%.3f, max=%.3f, avg=%.3f, stdDeviation=%.3f}", size, min, max, avg, stdDeviation );
    }
    
    public void writeTo( MetricStorage storage ) {
        storage.store( "count", size );
        storage.store( "min", min );
        storage.store( "max", max );
        storage.store( "avg", avg );
        storage.store( "std-dev", stdDeviation );
    }

    public static PopulationStatistics evaluate( Collection<Double> population ) {
        long size = population.size();
        double min = Collections.min( population );
        double max = Collections.max( population );
        double avg = population.stream().collect( Collectors.averagingDouble( l -> l ) );
        double stdDeviation = Math.sqrt( population.stream().mapToDouble( l -> Math.pow( l - avg, 2 ) ).average().getAsDouble() );

        return new PopulationStatistics( size, min, max, avg, stdDeviation );
    }
}
