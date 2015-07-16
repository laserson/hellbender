package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.AnnotatorCompatible;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.InfoFieldAnnotation;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;

import java.util.*;

/**
 * Likelihood of being a Mendelian Violation
 *
 * <p>This annotation uses the likelihoods of the genotype calls to assess whether a site is transmitted from parents to offspring according to Mendelian rules. The output is the likelihood of the site being a Mendelian violation, which can be tentatively interpreted either as an indication of error (in the genotype calls) or as a possible <em><de novo</em> mutation. The higher the output value, the more likely there is to be a Mendelian violation. Note that only positive values indicating likely MVs will be annotated; if the value for a given site is negative (indicating that there is no violation) the annotation is not written to the file.</p>
 *
 * <h3>Statistical notes</h3>
 * <p>This annotation considers all possible combinations of all possible genotypes (homozygous-reference, heterozygous, and homozygous-variant) for each member of a trio, which amounts to 27 possible combinations. Using the Phred-scaled genotype likelihoods (PL values) from each individual, the likelihood of each combination is calculated, and the result contributes to the likelihood of the corresponding case (mendelian violation or non-violation) depending on which set it belongs to. See the <a href="http://www.broadinstitute.org/gatk/guide/article?id=4732">method document on statistical tests</a> for a more detailed explanation of this statistical test.</p>
 *
 * <h3>Caveats</h3>
 * <ul>
 *     <li>The calculation assumes that the organism is diploid.</li>
 *     <li>This annotation requires a valid pedigree file.</li>
 *     <li>When multiple trios are present, the annotation is simply the maximum of the likelihood ratios, rather than the strict 1-Prod(1-p_i) calculation, as this can scale poorly for uncertain sites and many trios.</li>
 *     <li>This annotation can only be used from the VariantAnnotator. If you attempt to use it from the UnifiedGenotyper, the run will fail with an error message to that effect. If you attempt to use it from the HaplotypeCaller, the run will complete successfully but the annotation will not be added to any variants.</li>
 * </ul>
 *
 * <h3>Related annotations</h3>
 * <ul>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_PossibleDeNovo.php">PossibleDeNovo</a></b> annotates the existence of a de novo mutation in at least one of a set of families/trios.</li>
 * </ul>
 *
 */

public class MVLikelihoodRatio extends InfoFieldAnnotation implements RodRequiringAnnotation {

    private final static Logger logger = Logger.getLogger(MVLikelihoodRatio.class);
    private MendelianViolation mendelianViolation = null;
    private Set<Trio> trios;
    private boolean walkerIdentityCheckWarningLogged = false;
    private boolean pedigreeCheckWarningLogged = false;

    public Map<String, Object> annotate(final RefMetaDataTracker tracker,
                                        final AnnotatorCompatible walker,
                                        final ReferenceContext ref,
                                        final Map<String, AlignmentContext> stratifiedContexts,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {

        // Can only be called from VariantAnnotator
        if ( !(walker instanceof VariantAnnotator) ) {
            if ( !walkerIdentityCheckWarningLogged ) {
                if ( walker != null ) {
                    logger.warn("Annotation will not be calculated, must be called from VariantAnnotator, not " + walker.getClass().getName());
                } else {
                    logger.warn("Annotation will not be calculated, must be called from VariantAnnotator");
                }
                walkerIdentityCheckWarningLogged = true;
            }
            return null;
        }

        if ( mendelianViolation == null ) {
            // Must have a pedigree file
            trios = ((Walker) walker).getSampleDB().getTrios();
            if ( trios.isEmpty() ) {
                if ( !pedigreeCheckWarningLogged ) {
                    logger.warn("Annotation will not be calculated, mendelian violation annotation must provide a valid PED file (-ped) from the command line.");
                    pedigreeCheckWarningLogged = true;
                }
                return null;
            }
            mendelianViolation = new MendelianViolation(((VariantAnnotator)walker).minGenotypeQualityP );
        }

        final Map<String,Object> attributeMap = new HashMap<>(1);
        //double pNoMV = 1.0;
        double maxMVLR = Double.MIN_VALUE;
        for ( final Trio trio : trios ) {
            if ( contextHasTrioLikelihoods(vc,trio) ) {
                final Double likR = mendelianViolation.violationLikelihoodRatio(vc,trio.getMaternalID(),trio.getPaternalID(),trio.getChildID());
                maxMVLR = likR > maxMVLR ? likR : maxMVLR;
                //pNoMV *= (1.0-Math.pow(10.0,likR)/(1+Math.pow(10.0,likR)));
            }
        }

        //double pSomeMV = 1.0-pNoMV;
        //toRet.put("MVLR",Math.log10(pSomeMV)-Math.log10(1.0-pSomeMV));
        if ( Double.compare(maxMVLR, Double.MIN_VALUE) != 0 ) {
            attributeMap.put(getKeyNames().get(0), maxMVLR);
        }
        return attributeMap;
    }

    // return the descriptions used for the VCF INFO meta field
    @Override
    public List<String> getKeyNames() { return Arrays.asList(GATKVCFConstants.MENDEL_VIOLATION_LR_KEY); }

    @Override
    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(GATKVCFHeaderLines.getInfoLine(getKeyNames().get(0))); }

    private boolean contextHasTrioLikelihoods(final VariantContext context, final Trio trio) {
        for ( final String sample : Arrays.asList(trio.getMaternalID(), trio.getPaternalID(), trio.getChildID()) ) {
            if ( ! context.hasGenotype(sample) ) {
                return false;
            }
            if ( ! context.getGenotype(sample).hasLikelihoods() ) {
                return false;
            }
        }

        return true;
    }

}
