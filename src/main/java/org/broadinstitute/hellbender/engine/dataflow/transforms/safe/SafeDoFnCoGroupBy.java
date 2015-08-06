package org.broadinstitute.hellbender.engine.dataflow.transforms.safe;

import com.esotericsoftware.kryo.Kryo;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import org.objenesis.strategy.SerializingInstantiatorStrategy;


/**
 * Created by davidada on 7/31/15.
 */
public abstract class SafeDoFnCoGroupBy<O> extends DoFn<CoGbkResult, O> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
        Kryo kryo = new Kryo();
        kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());
        GATKProcessContext gatkProcessContext = new GATKProcessContext(c.element(), c);
        safeProcessElement(gatkProcessContext);
    }

    public abstract void safeProcessElement(GATKProcessContext c) throws Exception;

    public class GATKProcessContext {
        ProcessContext c;
        CoGbkResult copy;

        GATKProcessContext(CoGbkResult copy, ProcessContext c) {
            this.copy = copy;
            this.c = c;
        }

        public CoGbkResult element() {
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
