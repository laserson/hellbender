package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import org.broadinstitute.hellbender.engine.AlignmentContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.ActiveRegionBasedAnnotation;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.InfoFieldAnnotation;
import org.broadinstitute.hellbender.utils.MannWhitneyU;
import org.broadinstitute.hellbender.utils.QualityUtils;
import org.broadinstitute.hellbender.utils.genotyper.MostLikelyAllele;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.pileup.PileupElement;
import org.broadinstitute.hellbender.utils.pileup.ReadPileup;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.*;


/**
 * Abstract root for all RankSum based annotations
 */
public abstract class RankSumTest extends InfoFieldAnnotation implements ActiveRegionBasedAnnotation {
    private boolean useDithering = true;

    public RankSumTest(final boolean useDithering){
        this.useDithering = useDithering;
    }

    public RankSumTest(){
        this(true);
    }

    public Map<String, Object> annotate(final ReferenceContext ref,
                                        final Map<String, AlignmentContext> stratifiedContexts,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {
        if (stratifiedContexts == null && stratifiedPerReadAlleleLikelihoodMap == null){
            throw new IllegalArgumentException("either stratifiedContexts or stratifiedPerReadAlleleLikelihoodMap has to be non-null");
        }
        if (vc == null){
            return null;
        }
        final GenotypesContext genotypes = vc.getGenotypes();
        if (genotypes == null || genotypes.isEmpty() || stratifiedPerReadAlleleLikelihoodMap == null) {
            return null;
        }

        final List<Double> refQuals = new ArrayList<>();
        final List<Double> altQuals = new ArrayList<>();

        for ( final Genotype genotype : genotypes.iterateInSampleNameOrder() ) {
                final PerReadAlleleLikelihoodMap likelihoodMap = stratifiedPerReadAlleleLikelihoodMap.get(genotype.getSampleName());
                if ( likelihoodMap != null && !likelihoodMap.isEmpty() ) {
                    fillQualsFromLikelihoodMap(vc.getAlleles(), vc.getStart(), likelihoodMap, refQuals, altQuals);
                }
        }

        if ( refQuals.isEmpty() && altQuals.isEmpty() ) {
            return null;
        }

        // we are testing that set1 (the alt bases) have lower quality scores than set2 (the ref bases)
        final double p = MannWhitneyU.runOneSidedTest(useDithering, altQuals, refQuals).getLeft();
        if (Double.isNaN(p)) {
            return Collections.emptyMap();
        } else {
            return Collections.singletonMap(getKeyNames().get(0), String.format("%.3f", p));
        }
    }

    private void fillQualsFromLikelihoodMap(final List<Allele> alleles,
                                            final int refLoc,
                                            final PerReadAlleleLikelihoodMap likelihoodMap,
                                            final List<Double> refQuals,
                                            final List<Double> altQuals) {
        for ( final Map.Entry<GATKRead, Map<Allele,Double>> el : likelihoodMap.getLikelihoodReadMap().entrySet() ) {
            final MostLikelyAllele a = PerReadAlleleLikelihoodMap.getMostLikelyAllele(el.getValue());
            if ( ! a.isInformative() ) {
                continue; // read is non-informative
            }

            final GATKRead read = el.getKey();
            if ( isUsableRead(read, refLoc) ) {
                final Double value = getElementForRead(read, refLoc, a);
                if ( value == null ) {
                    continue;
                }

                if ( a.getMostLikelyAllele().isReference() ) {
                    refQuals.add(value);
                } else if ( alleles.contains(a.getMostLikelyAllele()) ) {
                    altQuals.add(value);
                }
            }
        }
    }

    /**
     * Get the element for the given read at the given reference position
     *
     * @param read     the read
     * @param refLoc   the reference position
     * @param mostLikelyAllele the most likely allele for this read
     * @return a Double representing the element to be used in the rank sum test, or null if it should not be used
     */
    protected Double getElementForRead(final GATKRead read, final int refLoc, final MostLikelyAllele mostLikelyAllele) {
        return getElementForRead(read, refLoc);
    }

    /**
     * Get the element for the given read at the given reference position
     *
     * @param read     the read
     * @param refLoc   the reference position
     * @return a Double representing the element to be used in the rank sum test, or null if it should not be used
     */
    protected abstract Double getElementForRead(final GATKRead read, final int refLoc);

    // TODO -- until the ReadPosRankSumTest stops treating these differently, we need to have separate methods for GATKSAMRecords and PileupElements.  Yuck.

    /**
     * Get the element for the given read at the given reference position
     *
     * By default this function returns null, indicating that the test doesn't support the old style of pileup calculations
     *
     * @param p        the pileup element
     * @return a Double representing the element to be used in the rank sum test, or null if it should not be used
     */
    protected Double getElementForPileupElement(final PileupElement p) {
        // does not work in pileup mode
        return null;
    }

    /**
     * Can the base in this pileup element be used in comparative tests between ref / alt bases?
     *
     * Note that this function by default does not allow deletion pileup elements
     *
     * @param p the pileup element to consider
     * @return true if this base is part of a meaningful read for comparison, false otherwise
     */
    protected boolean isUsableBase(final PileupElement p) {
        return !(p.isDeletion() ||
                 p.getMappingQual() == 0 ||
                 p.getMappingQual() == QualityUtils.MAPPING_QUALITY_UNAVAILABLE ||
                 ((int) p.getQual()) < QualityUtils.MIN_USABLE_Q_SCORE); // need the unBAQed quality score here
    }

    /**
     * Can the read be used in comparative tests between ref / alt bases?
     *
     * @param read   the read to consider
     * @param refLoc the reference location
     * @return true if this read is meaningful for comparison, false otherwise
     */
    protected boolean isUsableRead(final GATKRead read, final int refLoc) {
        return !( read.getMappingQuality() == 0 ||
                read.getMappingQuality() == QualityUtils.MAPPING_QUALITY_UNAVAILABLE );
    }
}