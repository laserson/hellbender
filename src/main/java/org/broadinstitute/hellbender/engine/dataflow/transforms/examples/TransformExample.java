package org.broadinstitute.hellbender.engine.dataflow.transforms.examples;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;
import org.broadinstitute.hellbender.engine.dataflow.transforms.safe.SafeDoFn;

/**
 * transformExample is a simple transform that takes Integers and produces a KV pair of ReferenceShard and the same
 * Integer. The purpose is to show the minimum pieces required to make a PTransform.
 */
public final class TransformExample extends PTransform<PCollection<Integer>, PCollection<KV<ReferenceShard, Integer>>> {
    private static final long serialVersionUID = 1L;
    Pipeline p;
    TransformExample(Pipeline p) {
        this.p = p;
    }
    @Override
    public PCollection<KV<ReferenceShard, Integer>> apply(PCollection<Integer> input) {
        return input.apply(ParDo.of(new SafeDoFn<Integer, KV<ReferenceShard, Integer>>(p.getCoderRegistry()) {
            @Override
            public void safeProcessElement(GATKProcessContext c) throws Exception {

            }
        }));
        /*
            return input.apply(ParDo.of(new DoFn<Integer, KV<ReferenceShard, Integer>>() {
                private static final long serialVersionUID = 1L;
                @Override
                public void processElement(ProcessContext c) throws Exception {
                    Integer i = c.element();
                    c.output(KV.of(new ReferenceShard(i.intValue(), i.toString()), i));
                }
            }).named("transformExample"));
            */
    }
}

