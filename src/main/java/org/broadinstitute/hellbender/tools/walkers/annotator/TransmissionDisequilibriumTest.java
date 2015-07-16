package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.AnnotatorCompatible;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.InfoFieldAnnotation;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;

import java.util.*;

/**
 * Wittkowski transmission disequilibrium test
 *
 * <p>This annotation indicates the presence of a genetic linkage between a genetic marker and a genetic trait.</p>
 *
 * <h3>Statistical notes</h3>
 * <p>The calculation is based on the derivation described in <a href="http://en.wikipedia.org/wiki/Transmission_disequilibrium_test#A_modified_version_of_the_TDT">http://en.wikipedia.org/wiki/Transmission_disequilibrium_test#A_modified_version_of_the_TDT</a>.</p>
 *
 * <h3>Caveat</h3>
 * <ul>
 *     <li>This annotation requires a valid pedigree file.</li>
 *     <li>This annotation can only be used with VariantAnnotator (not with UnifiedGenotyper or HaplotypeCaller).</li>
 * </ul>
 *
 */

public class TransmissionDisequilibriumTest extends InfoFieldAnnotation implements RodRequiringAnnotation {
    private final static Logger logger = Logger.getLogger(TransmissionDisequilibriumTest.class);
    private Set<Sample> trios = null;
    private final static int MIN_NUM_VALID_TRIOS = 5; // don't calculate this population-level statistic if there are less than X trios with full genotype likelihood information
    private boolean walkerIdentityCheckWarningLogged = false;
    private boolean pedigreeCheckWarningLogged = false;

    @Override
    public Map<String, Object> annotate(final RefMetaDataTracker tracker,
                                        final AnnotatorCompatible walker,
                                        final ReferenceContext ref,
                                        final Map<String, AlignmentContext> stratifiedContexts,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap){

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

        // Get trios from the input pedigree file.
        if ( trios == null ) {
            trios = ((VariantAnnotator) walker).getSampleDB().getChildrenWithParents();
            if (trios == null || trios.isEmpty()) {
                if ( !pedigreeCheckWarningLogged ) {
                    logger.warn("Transmission disequilibrium test annotation requires a valid ped file be passed in.");
                    pedigreeCheckWarningLogged = true;
                }
                return null;
            }
        }

        final Map<String, Object> toRet = new HashMap<>(1);
        final HashSet<Sample> triosToTest = new HashSet<>();

        for( final Sample child : trios ) {
            final boolean hasAppropriateGenotypes = vc.hasGenotype(child.getID()) && vc.getGenotype(child.getID()).hasLikelihoods() &&
                    vc.hasGenotype(child.getPaternalID()) && vc.getGenotype(child.getPaternalID()).hasLikelihoods() &&
                    vc.hasGenotype(child.getMaternalID()) && vc.getGenotype(child.getMaternalID()).hasLikelihoods();
            if ( hasAppropriateGenotypes ) {
                triosToTest.add(child);
            }
        }

        if( triosToTest.size() >= MIN_NUM_VALID_TRIOS ) {
            toRet.put("TDT", calculateTDT( vc, triosToTest ));
        }

        return toRet;
    }

    // return the descriptions used for the VCF INFO meta field
    @Override
    public List<String> getKeyNames() { return Arrays.asList(GATKVCFConstants.TRANSMISSION_DISEQUILIBRIUM_KEY); }

    @Override
    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(GATKVCFHeaderLines.getInfoLine(getKeyNames().get(0))); }

    // Following derivation in http://en.wikipedia.org/wiki/Transmission_disequilibrium_test#A_modified_version_of_the_TDT
    private List<Double> calculateTDT( final VariantContext vc, final Set<Sample> triosToTest ) {

        final List<Double> pairwiseTDTs = new ArrayList<>(10);
        final int HomRefIndex = 0;

        // for each pair of alleles, add the likelihoods
        final int numAltAlleles = vc.getAlternateAlleles().size();
        for ( int alt = 1; alt <= numAltAlleles; alt++ ) {
            final int HetIndex = alt;
            final int HomVarIndex = determineHomIndex(alt, numAltAlleles+1);

            final double nABGivenABandBB = calculateNChildren(vc, triosToTest, HetIndex, HetIndex, HomVarIndex) + calculateNChildren(vc, triosToTest, HetIndex, HomVarIndex, HetIndex);
            final double nBBGivenABandBB = calculateNChildren(vc, triosToTest, HomVarIndex, HetIndex, HomVarIndex) + calculateNChildren(vc, triosToTest, HomVarIndex, HomVarIndex, HetIndex);
            final double nAAGivenABandAB = calculateNChildren(vc, triosToTest, HomRefIndex, HetIndex, HetIndex);
            final double nBBGivenABandAB = calculateNChildren(vc, triosToTest, HomVarIndex, HetIndex, HetIndex);
            final double nAAGivenAAandAB = calculateNChildren(vc, triosToTest, HomRefIndex, HomRefIndex, HetIndex) + calculateNChildren(vc, triosToTest, HomRefIndex, HetIndex, HomRefIndex);
            final double nABGivenAAandAB = calculateNChildren(vc, triosToTest, HetIndex, HomRefIndex, HetIndex) + calculateNChildren(vc, triosToTest, HetIndex, HetIndex, HomRefIndex);

            final double numer = (nABGivenABandBB - nBBGivenABandBB) + 2.0 * (nAAGivenABandAB - nBBGivenABandAB) + (nAAGivenAAandAB - nABGivenAAandAB);
            final double denom = (nABGivenABandBB + nBBGivenABandBB) + 4.0 * (nAAGivenABandAB + nBBGivenABandAB) + (nAAGivenAAandAB + nABGivenAAandAB);
            pairwiseTDTs.add((numer * numer) / denom);
        }

        return pairwiseTDTs;
    }

    private double calculateNChildren( final VariantContext vc, final Set<Sample> triosToTest, final int childIdx, final int momIdx, final int dadIdx ) {
        final double likelihoodVector[] = new double[triosToTest.size()];
        int iii = 0;
        for( final Sample child : triosToTest ) {
            final double[] momGL = vc.getGenotype(child.getMaternalID()).getLikelihoods().getAsVector();
            final double[] dadGL = vc.getGenotype(child.getPaternalID()).getLikelihoods().getAsVector();
            final double[] childGL = vc.getGenotype(child.getID()).getLikelihoods().getAsVector();
            likelihoodVector[iii++] = momGL[momIdx] + dadGL[dadIdx] + childGL[childIdx];
        }

        return MathUtils.sumLog10(likelihoodVector);
    }
    
    private static int determineHomIndex(final int alleleIndex, int numAlleles) {
        int result = 0;
        for ( int i = 0; i < alleleIndex; i++ ) {
            result += numAlleles--;
        }
        return result;
    }
}
