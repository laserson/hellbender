package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.ActiveRegionBasedAnnotation;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.StandardAnnotation;
import org.broadinstitute.hellbender.utils.QualityUtils;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.commons.math3.util.CombinatoricsUtils.factorialLog;


/**
 * Strand bias estimated using Fisher's Exact Test
 *
 * <p>Strand bias is a type of sequencing bias in which one DNA strand is favored over the other, which can result in incorrect evaluation of the amount of evidence observed for one allele vs. the other. The FisherStrand annotation is one of several methods that aims to evaluate whether there is strand bias in the data. It uses Fisher's Exact Test to determine if there is strand bias between forward and reverse strands for the reference or alternate allele.”</p>
 * <p>The output is a Phred-scaled p-value. The higher the output value, the more likely there is to be bias. More bias is indicative of false positive calls.</p>
 *
 * <h3>Statistical notes</h3>
 * <p>See the <a href="http://www.broadinstitute.org/gatk/guide/article?id=4732">method document on statistical tests</a> for a more detailed explanation of this application of Fisher's Exact Test.</p>
 *
 * <h3>Caveats</h3>
 * <ul>
 *     <li>The FisherStrand test may not be calculated for certain complex indel cases or for multi-allelic sites.</li>
 *     <li>FisherStrand is best suited for low coverage situations. For testing strand bias in higher coverage situations, see the StrandOddsRatio annotation.</li>
 * </ul>
 * <h3>Related annotations</h3>
 * <ul>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_StrandBiasBySample.php">StrandBiasBySample</a></b> outputs counts of read depth per allele for each strand orientation.</li>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_StrandOddsRatio.php">StrandOddsRatio</a></b> is an updated form of FisherStrand that uses a symmetric odds ratio calculation.</li>
 * </ul>
 *
 */
public class FisherStrand extends StrandBiasTest implements StandardAnnotation, ActiveRegionBasedAnnotation {
    private final static boolean ENABLE_DEBUGGING = false;
    private final static Logger logger = LogManager.getLogger(FisherStrand.class);

    private static final double MIN_PVALUE = 1E-320;
    private static final int MIN_QUAL_FOR_FILTERED_TEST = 17;
    private static final int MIN_COUNT = ARRAY_DIM;

    @Override
    protected Map<String, Object> calculateAnnotationFromGTfield(final GenotypesContext genotypes){
        final int[][] tableFromPerSampleAnnotations = getTableFromSamples( genotypes, MIN_COUNT );
        return ( tableFromPerSampleAnnotations != null )? pValueForBestTable(tableFromPerSampleAnnotations, null) : null;
    }

    @Override
    protected Map<String, Object> calculateAnnotationFromStratifiedContexts(final Map<String, AlignmentContext> stratifiedContexts,
                                                                                     final VariantContext vc){
        final int[][] tableNoFiltering = getSNPContingencyTable(stratifiedContexts, vc.getReference(), vc.getAlternateAlleles(), -1, MIN_COUNT);
        final int[][] tableFiltering = getSNPContingencyTable(stratifiedContexts, vc.getReference(), vc.getAlternateAlleles(), MIN_QUAL_FOR_FILTERED_TEST, MIN_COUNT);
        printTable("unfiltered", tableNoFiltering);
        printTable("filtered", tableFiltering);
        return pValueForBestTable(tableFiltering, tableNoFiltering);
    }

    @Override
    protected Map<String, Object> calculateAnnotationFromLikelihoodMap(final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap,
                                                                                final VariantContext vc){
        // either SNP with no alignment context, or indels: per-read likelihood map needed
        final int[][] table = getContingencyTable(stratifiedPerReadAlleleLikelihoodMap, vc, MIN_COUNT);
        //logger.info("VC " + vc);
        //printTable(table, 0.0);
        return pValueForBestTable(table, null);
    }



    /**
     * Create an annotation for the highest (i.e., least significant) p-value of table1 and table2
     *
     * @param table1 a contingency table, may be null
     * @param table2 a contingency table, may be null
     * @return annotation result for FS given tables
     */
    private Map<String, Object> pValueForBestTable(final int[][] table1, final int[][] table2) {
        if ( table2 == null ) {
            return table1 == null ? null : annotationForOneTable(pValueForContingencyTable(table1));
        } else if (table1 == null) {
            return annotationForOneTable(pValueForContingencyTable(table2));
        } else { // take the one with the best (i.e., least significant pvalue)
            final double pvalue1 = pValueForContingencyTable(table1);
            final double pvalue2 = pValueForContingencyTable(table2);
            return annotationForOneTable(Math.max(pvalue1, pvalue2));
        }
    }

    /**
     * Returns an annotation result given a pValue
     *
     * @param pValue
     * @return a hash map from FS -> phred-scaled pValue
     */
    protected Map<String, Object> annotationForOneTable(final double pValue) {
        final Object value = String.format("%.3f", QualityUtils.phredScaleErrorRate(Math.max(pValue, MIN_PVALUE))); // prevent INFINITYs
        return Collections.singletonMap(getKeyNames().get(0), value);
    }

    @Override
    public List<String> getKeyNames() {
        return Collections.singletonList(GATKVCFConstants.FISHER_STRAND_KEY);
    }

    @Override
    public List<VCFInfoHeaderLine> getDescriptions() {
        return Collections.singletonList(GATKVCFHeaderLines.getInfoLine(getKeyNames().get(0)));
    }

    /**
     * Helper function to turn the FisherStrand table into the SB annotation array
     * @param table the table used by the FisherStrand annotation
     * @return the array used by the per-sample Strand Bias annotation
     */
    public static List<Integer> getContingencyArray( final int[][] table ) {
        if(table.length != ARRAY_DIM || table[0].length != ARRAY_DIM) {
            logger.warn("Expecting a " + ARRAY_DIM + "x" + ARRAY_DIM + " strand bias table.");
            return null;
        }

        final List<Integer> list = new ArrayList<>(ARRAY_SIZE); // TODO - if we ever want to do something clever with multi-allelic sites this will need to change
        list.add(table[0][0]);
        list.add(table[0][1]);
        list.add(table[1][0]);
        list.add(table[1][1]);
        return list;
    }

    public static Double pValueForContingencyTable(final int[][] originalTable) {
        final int[][] normalizedTable = normalizeContingencyTable(originalTable);

        int[][] table = copyContingencyTable(normalizedTable);

        final double pCutoff = computePValue(table);
        //printTable(table, pCutoff);

        double pValue = pCutoff;
        while (rotateTable(table)) {
            final double pValuePiece = computePValue(table);

            //printTable(table, pValuePiece);

            if (pValuePiece <= pCutoff) {
                pValue += pValuePiece;
            }
        }

        table = copyContingencyTable(normalizedTable);
        while (unrotateTable(table)) {
            final double pValuePiece = computePValue(table);

            //printTable(table, pValuePiece);

            if (pValuePiece <= pCutoff) {
                pValue += pValuePiece;
            }
        }

        //System.out.printf("P-cutoff: %f\n", pCutoff);
        //System.out.printf("P-value: %f\n\n", pValue);

        // min is necessary as numerical precision can result in pValue being slightly greater than 1.0
        return Math.min(pValue, 1.0);
    }

    // how large do we want the normalized table to be?
    private static final double TARGET_TABLE_SIZE = 200.0;

    /**
     * Normalize the table so that the entries are not too large.
     * Note that this method does NOT necessarily make a copy of the table being passed in!
     *
     * @param table  the original table
     * @return a normalized version of the table or the original table if it is already normalized
     */
    private static int[][] normalizeContingencyTable(final int[][] table) {
        final int sum = table[0][0] + table[0][1] + table[1][0] + table[1][1];
        if ( sum <= TARGET_TABLE_SIZE * 2 ) {
            return table;
        }

        final double normalizationFactor = (double)sum / TARGET_TABLE_SIZE;

        final int[][] normalized = new int[ARRAY_DIM][ARRAY_DIM];
        for ( int i = 0; i < ARRAY_DIM; i++ ) {
            for ( int j = 0; j < ARRAY_DIM; j++ ) {
                normalized[i][j] = (int) (table[i][j] / normalizationFactor);
            }
        }

        return normalized;
    }

    private static int [][] copyContingencyTable(final int [][] t) {
        final int[][] c = new int[ARRAY_DIM][ARRAY_DIM];

        for ( int i = 0; i < ARRAY_DIM; i++ ) {
            for (int j = 0; j < ARRAY_DIM; j++) {
                c[i][j] = t[i][j];
            }
        }

        return c;
    }


    private static void printTable(final int[][] table, final double pValue) {
        logger.info(String.format("%d %d; %d %d : %f", table[0][0], table[0][1], table[1][0], table[1][1], pValue));
    }

    /**
     * Printing information to logger.info for debugging purposes
     *
     * @param name the name of the table
     * @param table the table itself
     */
    private void printTable(final String name, final int[][] table) {
        if ( ENABLE_DEBUGGING ) {
            final String pValue = (String)annotationForOneTable(pValueForContingencyTable(table)).get(getKeyNames().get(0));
            logger.info(String.format("FS %s (REF+, REF-, ALT+, ALT-) = (%d, %d, %d, %d) = %s",
                    name, table[0][0], table[0][1], table[1][0], table[1][1], pValue));
        }
    }

    private static boolean rotateTable(final int[][] table) {
        table[0][0]--;
        table[1][0]++;

        table[0][1]++;
        table[1][1]--;

        return (table[0][0] >= 0 && table[1][1] >= 0);
    }

    private static boolean unrotateTable(final int[][] table) {
        table[0][0]++;
        table[1][0]--;

        table[0][1]--;
        table[1][1]++;

        return (table[0][1] >= 0 && table[1][0] >= 0);
    }

    private static double computePValue(final int[][] table) {

        final int[] rowSums = { sumRow(table, 0), sumRow(table, 1) };
        final int[] colSums = { sumColumn(table, 0), sumColumn(table, 1) };
        final int N = rowSums[0] + rowSums[1];

        // calculate in log space so we don't die with high numbers
        final double pCutoff = factorialLog(rowSums[0])
                + factorialLog(rowSums[1])
                + factorialLog(colSums[0])
                + factorialLog(colSums[1])
                - factorialLog(table[0][0])
                - factorialLog(table[0][1])
                - factorialLog(table[1][0])
                - factorialLog(table[1][1])
                - factorialLog(N);
        return Math.exp(pCutoff);
    }

    private static int sumRow(final int[][] table, final int column) {
        int sum = 0;
        for (int r = 0; r < table.length; r++) {
            sum += table[r][column];
        }

        return sum;
    }

    private static int sumColumn(final int[][] table, final int row) {
        int sum = 0;
        for (int c = 0; c < table[row].length; c++) {
            sum += table[row][c];
        }

        return sum;
    }



}
