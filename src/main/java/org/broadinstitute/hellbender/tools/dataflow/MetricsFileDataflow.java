package org.broadinstitute.hellbender.tools.dataflow;

import htsjdk.samtools.metrics.MetricBase;
import htsjdk.samtools.metrics.MetricsFile;

import java.io.Serializable;
import java.io.StringWriter;

public class MetricsFileDataflow<BEAN extends MetricBase & Serializable, HKEY extends Comparable<HKEY> & Serializable> extends MetricsFile<BEAN, HKEY> implements Serializable {
    public static final long serialVersionUID = 1l;

    @Override
    public String toString() {
        StringWriter writer = new StringWriter();
        write(writer);
        return writer.toString();
    }
}
