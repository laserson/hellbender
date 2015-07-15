package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.ActiveRegionBasedAnnotation;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.AnnotatorCompatible;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.InfoFieldAnnotation;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.StandardAnnotation;
import org.broadinstitute.hellbender.utils.MathUtils;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;

import java.util.*;


/**
 * Likelihood-based test for the inbreeding among samples
 *
 * <p>This annotation estimates whether there is evidence of inbreeding in a population. The higher the score, the higher the chance that there is inbreeding.</p>
 *
 * <h3>Statistical notes</h3>
 * <p>The calculation is a continuous generalization of the Hardy-Weinberg test for disequilibrium that works well with limited coverage per sample. The output is a Phred-scaled p-value derived from running the HW test for disequilibrium with PL values. See the <a href="http://www.broadinstitute.org/gatk/guide/article?id=4732">method document on statistical tests</a> for a more detailed explanation of this statistical test.</p>
 *
 * <h3>Caveats</h3>
 * <ul>
 * <li>The Inbreeding Coefficient can only be calculated for cohorts containing at least 10 founder samples.</li>
 * <li>This annotation is used in variant recalibration, but may not be appropriate for that purpose if the cohort being analyzed contains many closely related individuals.</li>
 * <li>This annotation requires a valid pedigree file.</li>
 * </ul>
 *
 */
public class InbreedingCoeff extends InfoFieldAnnotation implements StandardAnnotation, ActiveRegionBasedAnnotation {

    private final static Logger logger = Logger.getLogger(InbreedingCoeff.class);
    private static final int MIN_SAMPLES = 10;
    private Set<String> founderIds;
    private int sampleCount;
    private boolean pedigreeCheckWarningLogged = false;
    private boolean didUniquifiedSampleNameCheck = false;

    @Override
    public Map<String, Object> annotate(final RefMetaDataTracker tracker,
                                        final AnnotatorCompatible walker,
                                        final ReferenceContext ref,
                                        final Map<String, AlignmentContext> stratifiedContexts,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap ) {
        //If available, get the founder IDs and cache them. the IC will only be computed on founders then.
        if(founderIds == null && walker != null) {
            founderIds = ((Walker) walker).getSampleDB().getFounderIds();
        }
        //if none of the "founders" are in the vc samples, assume we uniquified the samples upstream and they are all founders
        if (!didUniquifiedSampleNameCheck) {
            checkSampleNames(vc);
            didUniquifiedSampleNameCheck = true;
        }
        if ( founderIds == null || founderIds.isEmpty() ) {
            if ( !pedigreeCheckWarningLogged ) {
                logger.warn("Annotation will not be calculated, must provide a valid PED file (-ped) from the command line.");
                pedigreeCheckWarningLogged = true;
            }
            return null;
        }
        else{
            return makeCoeffAnnotation(vc);
        }
    }

    protected double calculateIC(final VariantContext vc, final GenotypesContext genotypes) {

        final boolean doMultiallelicMapping = !vc.isBiallelic();

        int idxAA = 0, idxAB = 1, idxBB = 2;

        double refCount = 0.0;
        double hetCount = 0.0;
        double homCount = 0.0;
        sampleCount = 0; // number of samples that have likelihoods

        for ( final Genotype g : genotypes ) {
            if ( g.isCalled() && g.hasLikelihoods() && g.getPloidy() == 2)  // only work for diploid samples
                sampleCount++;
            else
                continue;
            final double[] normalizedLikelihoods = MathUtils.normalizeFromLog10(g.getLikelihoods().getAsVector());
            if (doMultiallelicMapping)
            {
                if (g.isHetNonRef()) {
                    //all likelihoods go to homCount
                    homCount++;
                    continue;
                }

                //get alternate allele for each sample
                final Allele a1 = g.getAllele(0);
                final Allele a2 = g.getAllele(1);
                if (a2.isNonReference()) {
                    final int[] idxVector = vc.getGLIndecesOfAlternateAllele(a2);
                    idxAA = idxVector[0];
                    idxAB = idxVector[1];
                    idxBB = idxVector[2];
                }
                //I expect hets to be reference first, but there are no guarantees (e.g. phasing)
                else if (a1.isNonReference()) {
                    final int[] idxVector = vc.getGLIndecesOfAlternateAllele(a1);
                    idxAA = idxVector[0];
                    idxAB = idxVector[1];
                    idxBB = idxVector[2];
                }
            }

            refCount += normalizedLikelihoods[idxAA];
            hetCount += normalizedLikelihoods[idxAB];
            homCount += normalizedLikelihoods[idxBB];
        }

        final double p = ( 2.0 * refCount + hetCount ) / ( 2.0 * (refCount + hetCount + homCount) ); // expected reference allele frequency
        final double q = 1.0 - p; // expected alternative allele frequency
        final double F = 1.0 - ( hetCount / ( 2.0 * p * q * (double) sampleCount) ); // inbreeding coefficient

        return F;
    }

    protected Map<String, Object> makeCoeffAnnotation(final VariantContext vc) {
        final GenotypesContext genotypes = (founderIds == null || founderIds.isEmpty()) ? vc.getGenotypes() : vc.getGenotypes(founderIds);
        if (genotypes == null || genotypes.size() < MIN_SAMPLES || !vc.isVariant())
            return null;
        double F = calculateIC(vc, genotypes);
        if (sampleCount < MIN_SAMPLES)
            return null;
        return Collections.singletonMap(getKeyNames().get(0), (Object) String.format("%.4f", F));
    }

    //this method is intended to reconcile uniquified sample names
    // it comes into play when calling this annotation from GenotypeGVCFs with --uniquifySamples because founderIds
    // is derived from the sampleDB, which comes from the input sample names, but vc will have uniquified (i.e. different)
    // sample names. Without this check, the founderIds won't be found in the vc and the annotation won't be calculated.
    protected void checkSampleNames(final VariantContext vc) {
        Set<String> vcSamples = new HashSet<>();
        vcSamples.addAll(vc.getSampleNames());
        if (!vcSamples.isEmpty()) {
            if (founderIds!=null) {
                vcSamples.removeAll(founderIds);
                if (vcSamples.equals(vc.getSampleNames()))
                    founderIds = vc.getSampleNames();
            }
        }
    }

    @Override
    public List<String> getKeyNames() { return Collections.singletonList(GATKVCFConstants.INBREEDING_COEFFICIENT_KEY); }

    @Override
    public List<VCFInfoHeaderLine> getDescriptions() { return Collections.singletonList(GATKVCFHeaderLines.getInfoLine(getKeyNames().get(0))); }
}