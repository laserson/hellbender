package org.broadinstitute.hellbender.engine.spark;

import com.esotericsoftware.kryo.Kryo;
import com.google.api.services.genomics.model.Read;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.spark.serializer.KryoRegistrator;

import java.util.Collections;

public class GATKRegistrator implements KryoRegistrator {
    @SuppressWarnings("unchecked")
    @Override
    public void registerClasses(Kryo kryo) {

        kryo.register(Read.class, new JsonSerializer<Read>());
        // htsjdk.variant.variantcontext.CommonInfo has a Map<String, Object> that defaults to
        // a Collections.unmodifiableMap. This can't be handled by the version of kryo used in Spark, it's fixed
        // in newer versions, but we can't use those because of incompatibility with Spark. We just include the fix
        // here.
        kryo.register(Collections.unmodifiableMap(Collections.EMPTY_MAP).getClass(), new UnmodifiableCollectionsSerializer());
    }
}