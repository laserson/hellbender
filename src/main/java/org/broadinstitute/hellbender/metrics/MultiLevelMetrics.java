package org.broadinstitute.hellbender.metrics;

import org.broadinstitute.hellbender.tools.dataflow.transforms.metrics.SerializableMetric;

import java.util.Comparator;

public abstract class MultiLevelMetrics extends SerializableMetric{
    private static final long serialVersionUID = 1l;

    /** The sample to which these metrics apply.  If null, it means they apply
     * to all reads in the file. */
    public String SAMPLE;

    /** The library to which these metrics apply.  If null, it means that the
     * metrics were accumulated at the sample level. */
    public String LIBRARY;

    /** The read group to which these metrics apply.  If null, it means that
     * the metrics were accumulated at the library or sample level.*/
    public String READ_GROUP;

    public static <T extends MultiLevelMetrics> Comparator<T> getMultiLevelMetricsComparator(){
        return Comparator.comparing((T a) -> a.SAMPLE != null ? a.SAMPLE : "")
            .thenComparing(a -> a.READ_GROUP != null ? a.READ_GROUP : "")
            .thenComparing(a -> a.LIBRARY != null ? a.LIBRARY : "");
    }


    //TODO: remove these methods once htsjdk is updated to 1.138
    //BEGIN: Nasty hack to get around equality + hashcode bug
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return this.toString().equals(o.toString());
    }

    //since equals relies on toString subclasses MUST NOT override to string
    @Override
    public final String toString(){
        return super.toString();
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
    //END: NastyHack
}
