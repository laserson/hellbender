package org.broadinstitute.hellbender.engine.dataflow.transforms.safe;

import com.esotericsoftware.kryo.Kryo;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;


/**
 * Created by davidada on 7/31/15.
 */
public abstract class SafeDoFn<I, O> extends DoFn<I, O> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
        Kryo kryo = new Kryo();
        I copy = kryo.copy(c.element());
        GATKProcessContext gatkProcessContext = new GATKProcessContext(copy, c);
        safeProcessElement(gatkProcessContext);
    }

    public abstract void safeProcessElement(GATKProcessContext c) throws Exception;

    public class GATKProcessContext {
        DoFn<I, O>.ProcessContext c;
        I copy;

        GATKProcessContext(I copy, DoFn<I, O>.ProcessContext c) {
            this.copy = copy;
            this.c = c;
        }

        public I element() {
            return c.element();
        }

        public <T> T sideInput(PCollectionView<T> view) {
            return c.sideInput(view);
        }

        public PipelineOptions getPipelineOptions() {
            return c.getPipelineOptions();
        }

        public void output(O output) {
            c.output(output);
        }

        public <T> void sideOutput(TupleTag<T> tag, T output) {
            c.sideOutput(tag, output);
        }
    }

}
