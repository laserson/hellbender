package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.engine.AlignmentContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.AnnotatorCompatible;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.InfoFieldAnnotation;
import org.broadinstitute.hellbender.utils.QualityUtils;
import org.broadinstitute.hellbender.utils.genotyper.MostLikelyAllele;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.pileup.PileupElement;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Class of tests to detect strand bias.
 */
public abstract class StrandBiasTest extends InfoFieldAnnotation {
    private final static Logger logger = LogManager.getLogger(StrandBiasTest.class);
    private static boolean stratifiedPerReadAlleleLikelihoodMapWarningLogged = false;
    private static boolean inputVariantContextWarningLogged = false;
    private static boolean getTableFromSamplesWarningLogged = false;
    private static boolean decodeSBBSWarningLogged = false;

    protected static final int ARRAY_DIM = 2;
    protected static final int ARRAY_SIZE = ARRAY_DIM * ARRAY_DIM;

    StrandBiasTest(final Set<VCFHeaderLine> headerLines){
        // Does the VCF header contain strand bias (SB) by sample annotation?
        for ( final VCFHeaderLine line : headerLines) {
            if ( line instanceof VCFFormatHeaderLine) {
                final VCFFormatHeaderLine formatline = (VCFFormatHeaderLine)line;
                if ( formatline.getID().equals(GATKVCFConstants.STRAND_BIAS_BY_SAMPLE_KEY) ) {
                    logger.warn("StrandBiasBySample annotation exists in input VCF header. Attempting to use StrandBiasBySample " +
                            "values to calculate strand bias annotation values. If no sample has the SB genotype annotation, annotation may still fail.");
                }
            }
        }
    }

    @Override
    //template method for calculating strand bias annotations using the three different methods
    public Map<String, Object> annotate(final AnnotatorCompatible walker,
                                        final ReferenceContext ref,
                                        final Map<String,AlignmentContext> stratifiedContexts,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {

        // do not process if not a variant
        if ( !vc.isVariant() ) {
            return null;
        }

        // if the genotype and strand bias are provided, calculate the annotation from the Genotype (GT) field
        if ( vc.hasGenotypes() ) {
            for (final Genotype g : vc.getGenotypes()) {
                if (g.hasAnyAttribute(GATKVCFConstants.STRAND_BIAS_BY_SAMPLE_KEY)) {
                    return calculateAnnotationFromGTfield(vc.getGenotypes());
                }
            }
        }

        // if a the variant is a snp and has stratified contexts, calculate the annotation from the stratified contexts
        //stratifiedContexts can come come from VariantAnnotator, but will be empty if no reads were provided
        if (vc.isSNP() && stratifiedContexts != null  && !stratifiedContexts.isEmpty()) {
            return calculateAnnotationFromStratifiedContexts(stratifiedContexts, vc);
        }

        // calculate the annotation from the stratified per read likelihood map
        // stratifiedPerReadAllelelikelihoodMap can come from HaplotypeCaller call to VariantAnnotatorEngine
        else if (stratifiedPerReadAlleleLikelihoodMap != null) {
            return calculateAnnotationFromLikelihoodMap(stratifiedPerReadAlleleLikelihoodMap, vc);
        }
        else {
            // for non-snp variants, we need per-read likelihoods.
            // for snps, we can get same result from simple pileup
            // for indels that do not have a computed strand bias (SB) or strand bias by sample (SBBS)
            return null;
        }
    }

    protected abstract Map<String, Object> calculateAnnotationFromGTfield(final GenotypesContext genotypes);

    protected abstract Map<String, Object> calculateAnnotationFromStratifiedContexts(final Map<String, AlignmentContext> stratifiedContexts,
                                                                                     final VariantContext vc);

    protected abstract Map<String, Object> calculateAnnotationFromLikelihoodMap(final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap,
                                                                                final VariantContext vc);

    /**
     * Create the contingency table by retrieving the per-sample strand bias annotation and adding them together
     * @param genotypes the genotypes from which to pull out the per-sample strand bias annotation
     * @param minCount minimum threshold for the sample strand bias counts for each ref and alt.
     *                 If both ref and alt counts are above minCount the whole sample strand bias is added to the resulting table
     * @return the table used for several strand bias tests, will be null if none of the genotypes contain the per-sample SB annotation
     */
    protected int[][] getTableFromSamples( final GenotypesContext genotypes, final int minCount ) {
        if( genotypes == null ) {
            if ( !getTableFromSamplesWarningLogged ) {
                logger.warn("Genotypes cannot be null.");
                getTableFromSamplesWarningLogged = true;
            }
            return null;
        }

        final int[] sbArray = {0,0,0,0}; // reference-forward-reverse -by- alternate-forward-reverse
        boolean foundData = false;

        for( final Genotype g : genotypes ) {
            if( g.isNoCall() || ! g.hasAnyAttribute(GATKVCFConstants.STRAND_BIAS_BY_SAMPLE_KEY) ) {
                continue;
            }

            foundData = true;
            final String sbbsString = (String) g.getAnyAttribute(GATKVCFConstants.STRAND_BIAS_BY_SAMPLE_KEY);
            final int[] data = encodeSBBS(sbbsString);
            if ( passesMinimumThreshold(data, minCount) ) {
                for( int index = 0; index < sbArray.length; index++ ) {
                    sbArray[index] += data[index];
                }
            }
        }

        return ( foundData ? decodeSBBS(sbArray) : null );
    }

    /**
     Allocate and fill a 2x2 strand contingency table.  In the end, it'll look something like this:
     *             fw      rc
     *   allele1   #       #
     *   allele2   #       #
     * @return a 2x2 contingency table
     */
    protected static int[][] getSNPContingencyTable(final Map<String, AlignmentContext> stratifiedContexts,
                                                  final Allele ref,
                                                  final List<Allele> allAlts,
                                                  final int minQScoreToConsider,
                                                  final int minCount ) {
        final int[][] table = new int[ARRAY_DIM][ARRAY_DIM];

        for (final Map.Entry<String, AlignmentContext> sample : stratifiedContexts.entrySet() ) {
            final int[] myTable = new int[ARRAY_SIZE];
            for (final PileupElement p : sample.getValue().getBasePileup()) {

                if ( ! isUsableBase(p) ) // ignore deletions and bad MQ
                {
                    continue;
                }

                if ( p.getQual() < minQScoreToConsider || p.getMappingQual() < minQScoreToConsider ) {
                    continue;
                }

                updateTable(myTable, Allele.create(p.getBase(), false), p.getRead(), ref, allAlts);
            }

            if ( passesMinimumThreshold( myTable, minCount ) ) {
                copyToMainTable(myTable, table);
            }

        }

        return table;
    }

    /**
     Allocate and fill a 2x2 strand contingency table.  In the end, it'll look something like this:
     *             fw      rc
     *   allele1   #       #
     *   allele2   #       #
     * @return a 2x2 contingency table
     */
    public static int[][] getContingencyTable( final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap,
                                               final VariantContext vc,
                                               final int minCount) {
        if( stratifiedPerReadAlleleLikelihoodMap == null ) {
            if ( !stratifiedPerReadAlleleLikelihoodMapWarningLogged ) {
                logger.warn("stratifiedPerReadAlleleLikelihoodMap cannot be null");
                stratifiedPerReadAlleleLikelihoodMapWarningLogged = true;
            }
            return null;
        }
        if( vc == null ) {
            if ( !inputVariantContextWarningLogged ) {
                logger.warn("input vc cannot be null");
                inputVariantContextWarningLogged = true;
            }
            return null;
        }

        final Allele ref = vc.getReference();
        final Allele alt = vc.getAltAlleleWithHighestAlleleCount();
        final List<Allele> allAlts = vc.getAlternateAlleles();
        final int[][] table = new int[ARRAY_DIM][ARRAY_DIM];

        for (final PerReadAlleleLikelihoodMap maps : stratifiedPerReadAlleleLikelihoodMap.values() ) {
            final int[] myTable = new int[ARRAY_SIZE];
            for (final Map.Entry<GATKRead,Map<Allele,Double>> el : maps.getLikelihoodReadMap().entrySet()) {
                final MostLikelyAllele mostLikelyAllele = PerReadAlleleLikelihoodMap.getMostLikelyAllele(el.getValue());
                final GATKRead read = el.getKey();
                updateTable(myTable, mostLikelyAllele.getAlleleIfInformative(), read, ref, allAlts);
            }
            if ( passesMinimumThreshold(myTable, minCount) ) {
                copyToMainTable(myTable, table);
            }
        }

        return table;
    }

    /**
     * Helper method to copy the per-sample table to the main table
     *
     * @param perSampleTable   per-sample table (single dimension)
     * @param mainTable        main table (two dimensions)
     */
    private static void copyToMainTable(final int[] perSampleTable, final int[][] mainTable) {
        mainTable[0][0] += perSampleTable[0];
        mainTable[0][1] += perSampleTable[1];
        mainTable[1][0] += perSampleTable[2];
        mainTable[1][1] += perSampleTable[3];
    }


    /**
     * Can the base in this pileup element be used in comparative tests?
     *
     * @param p the pileup element to consider
     *
     * @return true if this base is part of a meaningful read for comparison, false otherwise
     */
    private static boolean isUsableBase(final PileupElement p) {
        return !( p.isDeletion() ||
                p.getMappingQual() == 0 ||
                p.getMappingQual() == QualityUtils.MAPPING_QUALITY_UNAVAILABLE ||
                ((int) p.getQual()) < QualityUtils.MIN_USABLE_Q_SCORE);
    }

    private static void updateTable(final int[] table, final Allele allele, final GATKRead read, final Allele ref, final List<Allele> allAlts) {

        final boolean matchesRef = allele.equals(ref, true);
        final boolean matchesAlt = allele.equals(allAlts.get(0), true);
        final boolean matchesAnyAlt = allAlts.contains(allele);

        if ( matchesRef || matchesAnyAlt ) {
            final int offset = matchesRef ? 0 : ARRAY_DIM;

            // a normal read with an actual strand
            final boolean isFW = !read.isReverseStrand();
            table[offset + (isFW ? 0 : 1)]++;
        }
    }

    /**
     * Does this strand data array pass the minimum threshold for inclusion?
     *
     * @param data  the array
     * @param minCount The minimum threshold of counts in the array
     * @return true if it passes the minimum threshold, false otherwise
     */
    protected static boolean passesMinimumThreshold(final int[] data, final int minCount) {
        // the ref and alt totals must be greater than MIN_COUNT
        return data[0] + data[1] + data[2] + data[3] > minCount;
    }

    /**
     * Helper function to parse the genotype annotation into the SB annotation array
     * @param string the string that is returned by genotype.getAnnotation("SB")
     * @return the array used by the per-sample Strand Bias annotation
     */
    private static int[] encodeSBBS( final String string ) {
        final int[] array = new int[ARRAY_SIZE];
        final StringTokenizer tokenizer = new StringTokenizer(string, ",", false);
        for( int index = 0; index < ARRAY_SIZE; index++ ) {
            array[index] = Integer.parseInt(tokenizer.nextToken());
        }
        return array;
    }

    /**
     * Helper function to turn the  SB annotation array into a contingency table
     * @param array the array used by the per-sample Strand Bias annotation
     * @return the table used by the StrandOddsRatio annotation
     */
    private static int[][] decodeSBBS( final int[] array ) {
        if(array.length != ARRAY_SIZE) {
            if ( !decodeSBBSWarningLogged ) {
                logger.warn("Expecting a length = " +  ARRAY_SIZE + " strand bias array.");
                decodeSBBSWarningLogged = true;
            }
            return null;
        }
        final int[][] table = new int[ARRAY_DIM][ARRAY_DIM];
        table[0][0] = array[0];
        table[0][1] = array[1];
        table[1][0] = array[2];
        table[1][1] = array[3];
        return table;
    }
}
