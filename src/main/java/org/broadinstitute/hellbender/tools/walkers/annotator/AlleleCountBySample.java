package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.AnnotatorCompatible;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.GenotypeAnnotation;
import org.broadinstitute.hellbender.utils.MathUtils;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Allele count and frequency expectation per sample
 *
 * <p>This annotation calculates the maximum likelihood (ML) number and frequency of alternate alleles for each individual sample at a site. In essence, it is equivalent to calculating the sum of "1"s in a genotype (for a biallelic site).</p>
 *
 */
public final class AlleleCountBySample extends GenotypeAnnotation {

    private final static List<String> keyNames = Collections.unmodifiableList(Arrays.asList(GATKVCFConstants.MLE_PER_SAMPLE_ALLELE_COUNT_KEY, GATKVCFConstants.MLE_PER_SAMPLE_ALLELE_FRACTION_KEY));

    private final static List<VCFFormatHeaderLine> descriptors = Collections.unmodifiableList(Arrays.asList(
            GATKVCFHeaderLines.getFormatLine(GATKVCFConstants.MLE_PER_SAMPLE_ALLELE_COUNT_KEY),
            GATKVCFHeaderLines.getFormatLine(GATKVCFConstants.MLE_PER_SAMPLE_ALLELE_FRACTION_KEY)
    ));

    @Override
    public void annotate(final RefMetaDataTracker tracker,
                         final AnnotatorCompatible walker,
                         final ReferenceContext ref,
                         final AlignmentContext stratifiedContext,
                         final VariantContext vc,
                         final Genotype g,
                         final GenotypeBuilder gb,
                         final PerReadAlleleLikelihoodMap alleleLikelihoodMap) {

        if (!g.hasPL()) {
            return;
        }
        final int[] PL = g.getPL();
        if (PL.length == 0) {
            return;
        }

        final int bestLikelihoodIndex = MathUtils.minElementIndex(PL);
        final int numberOfAlleles = vc.getNAlleles();
        final int ploidy = g.getPloidy();
        final GenotypeLikelihoodCalculator calculator = GenotypeLikelihoodCalculators.getInstance(ploidy, numberOfAlleles);
        final GenotypeAlleleCounts genotypeAlleleCounts = calculator.genotypeAlleleCountsAt(bestLikelihoodIndex);
        final int[] AC = new int[numberOfAlleles - 1];
        final double[] AF = new double[numberOfAlleles - 1];
        final int allelesPresentCount = genotypeAlleleCounts.distinctAlleleCount();
        for (int i = 0; i < allelesPresentCount; i++) {
            final int alleleIndex = genotypeAlleleCounts.alleleIndexAt(i);
            if (alleleIndex == 0) {
                continue; // skip the reference allele.
            }
            final int alleleCount = genotypeAlleleCounts.alleleCountAt(i);
            AC[alleleIndex - 1] = alleleCount;
            AF[alleleIndex - 1] = ((double) alleleCount) / (double) ploidy;
        }
        gb.attribute(GATKVCFConstants.MLE_PER_SAMPLE_ALLELE_COUNT_KEY, AC);
        gb.attribute(GATKVCFConstants.MLE_PER_SAMPLE_ALLELE_FRACTION_KEY, AF);
    }

    @Override
    public List<VCFFormatHeaderLine> getDescriptions() {
        return descriptors;
    }

    @Override
    public List<String> getKeyNames() {
        return keyNames;
    }
}
