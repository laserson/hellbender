package org.broadinstitute.hellbender.engine.dataflow.transforms.safe;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.sun.org.apache.bcel.internal.generic.INSTANCEOF;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Created by davidada on 7/31/15.
 */
public abstract class SafeDoFn<I, O> extends DoFn<I, O> {
    Coder<I> coder;
    public SafeDoFn(Pipeline p) {
        Class<I> clazz = null;
        I i = null;
        try {
            i = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        try {
            this.coder = p.getCoderRegistry().getDefaultCoder(i);
        } catch (CannotProvideCoderException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        coder.encode(c.element(), byteOutputStream, Coder.Context.OUTER);

        byte[] bytes = byteOutputStream.toByteArray();
        ByteArrayInputStream byteInputStream = new ByteArrayInputStream(bytes);

        I copy = coder.decode(byteInputStream, Coder.Context.OUTER);

        GATKProcessContext gatkProcessContext = new GATKProcessContext(copy, c);
        safeProcessElement(gatkProcessContext);
    }

    public abstract void safeProcessElement(GATKProcessContext c) throws Exception;

    public class GATKProcessContext extends AbstractGATKProcessContext<I, O> {
        GATKProcessContext(I copy, ProcessContext c) {
            super(copy, c);
        }
    }
}
