package org.broadinstitute.hellbender.tools.walkers.annotator.interfaces;

import htsjdk.variant.vcf.VCFHeaderLine;

import java.util.List;
import java.util.Set;

public abstract class VariantAnnotatorAnnotation {
    // return the INFO keys
    public abstract List<String> getKeyNames();

    // initialization method (optional for subclasses, and therefore non-abstract)
    public void initialize ( final AnnotatorCompatible walker, final GenomeAnalysisEngine toolkit, final Set<VCFHeaderLine> headerLines ) { }
}