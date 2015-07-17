package org.broadinstitute.hellbender.tools.walkers.annotator.interfaces;

import java.util.List;

public abstract class VariantAnnotatorAnnotation {
    // return the INFO keys
    public abstract List<String> getKeyNames();
}