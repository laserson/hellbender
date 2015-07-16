package org.broadinstitute.hellbender.tools.walkers.annotator.interfaces;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.broadinstitute.hellbender.engine.AlignmentContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class InfoFieldAnnotation extends VariantAnnotatorAnnotation {
    // return annotations for the given contexts split by sample
    public Map<String, Object> annotate(final AnnotatorCompatible walker,
                                        final ReferenceContext ref,
                                        final Map<String, AlignmentContext> stratifiedContexts,
                                        final VariantContext vc) {
        return annotate(walker, ref, stratifiedContexts, vc, null);
    }

    public Map<String, Object> annotate(final Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap, final VariantContext vc) {
        return annotate(null, null, null, vc, perReadAlleleLikelihoodMap);
    }

    public Map<String, Object> annotate(final ReferenceContext referenceContext, final Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap, final VariantContext vc) {

        return annotate(null, referenceContext, null, vc, perReadAlleleLikelihoodMap);
    }

    public abstract Map<String, Object> annotate(final AnnotatorCompatible walker,
                                                 final ReferenceContext ref,
                                                 final Map<String, AlignmentContext> stratifiedContexts,
                                                 final VariantContext vc,
                                                 final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap);

    // return the descriptions used for the VCF INFO meta field
    public List<VCFInfoHeaderLine> getDescriptions() {
        final List<VCFInfoHeaderLine> lines = new ArrayList<>(5);
        for (final String key : getKeyNames()) {
            lines.add(GATKVCFHeaderLines.getInfoLine(key));
        }
        return lines;
    }
}