package org.broadinstitute.hellbender.metrics;

import htsjdk.samtools.metrics.MetricBase;

import java.io.Serializable;
import java.util.Comparator;

public abstract class MultiLevelMetrics extends MetricBase implements Serializable{
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

    private static final Comparator<MultiLevelMetrics> insertSizeSorting = Comparator.comparing((MultiLevelMetrics a) -> a.SAMPLE != null ? a.SAMPLE : "")
            .thenComparing(a -> a.READ_GROUP != null ? a.READ_GROUP : "")
            .thenComparing(a -> a.LIBRARY != null ? a.LIBRARY : "");

    public static Comparator<MultiLevelMetrics> getComparator() {
        return insertSizeSorting;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        //if (!super.equals(o)) return false;

        MultiLevelMetrics that = (MultiLevelMetrics) o;

        if (SAMPLE != null ? !SAMPLE.equals(that.SAMPLE) : that.SAMPLE != null) return false;
        if (LIBRARY != null ? !LIBRARY.equals(that.LIBRARY) : that.LIBRARY != null) return false;
        return !(READ_GROUP != null ? !READ_GROUP.equals(that.READ_GROUP) : that.READ_GROUP != null);

    }

    @Override
    public int hashCode() {
        //int result = super.hashCode();
        int result = 0;
        result = 31 * result + (SAMPLE != null ? SAMPLE.hashCode() : 0);
        result = 31 * result + (LIBRARY != null ? LIBRARY.hashCode() : 0);
        result = 31 * result + (READ_GROUP != null ? READ_GROUP.hashCode() : 0);
        return result;
    }
}
