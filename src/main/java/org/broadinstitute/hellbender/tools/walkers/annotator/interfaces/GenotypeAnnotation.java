package org.broadinstitute.hellbender.tools.walkers.annotator.interfaces;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;

import java.util.List;

public abstract class GenotypeAnnotation extends VariantAnnotatorAnnotation {

    // return annotations for the given contexts/genotype split by sample
    public abstract void annotate(final RefMetaDataTracker tracker,
                                  final AnnotatorCompatible walker,
                                  final ReferenceContext ref,
                                  final AlignmentContext stratifiedContext,
                                  final VariantContext vc,
                                  final Genotype g,
                                  final GenotypeBuilder gb,
                                  final PerReadAlleleLikelihoodMap alleleLikelihoodMap);

    // return the descriptions used for the VCF FORMAT meta field
    public abstract List<VCFFormatHeaderLine> getDescriptions();

}