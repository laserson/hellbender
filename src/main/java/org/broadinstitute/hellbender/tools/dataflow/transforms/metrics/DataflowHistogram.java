package org.broadinstitute.hellbender.tools.dataflow.transforms.metrics;

import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import htsjdk.samtools.util.Histogram;

@DefaultCoder(SerializableCoder.class)
public class DataflowHistogram<K extends Comparable<K>> extends Histogram<K> implements Combine.AccumulatingCombineFn.Accumulator<K, DataflowHistogram<K>, DataflowHistogram<K>>{
    public final static long serialVersionUID = 1l;

    @Override
    public void addInput(K input) {
        this.increment(input);
    }

    @Override
    public void mergeAccumulator(DataflowHistogram<K> other) {
        this.addHistogram(other);
    }

    @Override
    public DataflowHistogram<K> extractOutput() {
        return this;
    }

    @Override
    public String toString(){
        return "Histogram: #bins=" + this.size() + " mean=" + this.getMean();
    }


}

