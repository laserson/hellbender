package org.broadinstitute.hellbender.tools.walkers.annotator.interfaces;

import htsjdk.variant.variantcontext.VariantContext;
import org.broadinstitute.hellbender.engine.FeatureInput;

import java.util.List;

public interface AnnotatorCompatible {

    // getter methods for various used bindings
    public FeatureInput<VariantContext> getSnpEffRodBinding();
    public FeatureInput<VariantContext> getDbsnpRodBinding();
    public List<FeatureInput<VariantContext>> getCompRodBindings();
    public List<FeatureInput<VariantContext>> getResourceRodBindings();
    public boolean alwaysAppendDbsnpId();
}
