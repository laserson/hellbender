package org.broadinstitute.hellbender.tools.walkers.annotator;

import com.google.common.annotations.VisibleForTesting;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.broadinstitute.hellbender.engine.AlignmentContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.ActiveRegionBasedAnnotation;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.StandardAnnotation;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Strand bias estimated by the Symmetric Odds Ratio test
 *
 * <p>Strand bias is a type of sequencing bias in which one DNA strand is favored over the other, which can result in incorrect evaluation of the amount of evidence observed for one allele vs. the other. The StrandOddsRatio annotation is one of several methods that aims to evaluate whether there is strand bias in the data. It is an updated form of the Fisher Strand Test that is better at taking into account large amounts of data in high coverage situations. It is used to determine if there is strand bias between forward and reverse strands for the reference or alternate allele.</p>
 *
 * <h3>Statistical notes</h3>
 * <p> Odds Ratios in the 2x2 contingency table below are</p>
 *
 * $$ R = \frac{X[0][0] * X[1][1]}{X[0][1] * X[1][0]} $$
 *
 * <p>and its inverse:</p>
 *
 * <table>
 *      <tr><td>&nbsp;</td><td>+ strand </td><td>- strand</td></tr>
 *      <tr><td>REF;</td><td>X[0][0]</td><td>X[0][1]</td></tr>
 *      <tr><td>ALT;</td><td>X[1][0]</td><td>X[1][1]</td></tr>
 * </table>
 *
 * <p>The sum R + 1/R is used to detect a difference in strand bias for REF and for ALT (the sum makes it symmetric). A high value is indicative of large difference where one entry is very small compared to the others. A scale factor of refRatio/altRatio where</p>
 *
 * $$ refRatio = \frac{max(X[0][0], X[0][1])}{min(X[0][0], X[0][1} $$
 *
 * <p>and </p>
 *
 * $$ altRatio = \frac{max(X[1][0], X[1][1])}{min(X[1][0], X[1][1]} $$
 *
 * <p>ensures that the annotation value is large only. </p>
 *
 * <p>See the <a href="http://www.broadinstitute.org/gatk/guide/article?id=4732">method document on statistical tests</a> for a more detailed explanation of this statistical test.</p>
 *
 * <h3>Related annotations</h3>
 * <ul>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_StrandBiasBySample.php">StrandBiasBySample</a></b> outputs counts of read depth per allele for each strand orientation.</li>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_FisherStrand.php">FisherStrand</a></b> uses Fisher's Exact Test to evaluate strand bias.</li>
 * </ul>
 *
 */
public final class StrandOddsRatio extends StrandBiasTest implements StandardAnnotation, ActiveRegionBasedAnnotation {
    private static final double AUGMENTATION_CONSTANT = 1.0;
    private static final int MIN_COUNT = 0;

    StrandOddsRatio(final Set<VCFHeaderLine> headerLines){
        super(headerLines);
    }

    @Override
    protected Map<String, Object> calculateAnnotationFromGTfield(final GenotypesContext genotypes){
        final int[][] tableFromPerSampleAnnotations = getTableFromSamples( genotypes, MIN_COUNT );
        return tableFromPerSampleAnnotations != null ? annotationForOneTable(calculateSOR(tableFromPerSampleAnnotations)) : null;
    }

    @Override
    protected Map<String, Object> calculateAnnotationFromStratifiedContexts(final Map<String, AlignmentContext> stratifiedContexts, final VariantContext vc){
        final int[][] tableNoFiltering = getSNPContingencyTable(stratifiedContexts, vc.getReference(), vc.getAlternateAlleles(), -1, MIN_COUNT);
        return annotationForOneTable(calculateSOR(tableNoFiltering));
    }

    @Override
    protected Map<String, Object> calculateAnnotationFromLikelihoodMap(final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap, final VariantContext vc){
        // either SNP with no alignment context, or indels: per-read likelihood map needed
        final int[][] table = getContingencyTable(stratifiedPerReadAlleleLikelihoodMap, vc, MIN_COUNT);
        return annotationForOneTable(calculateSOR(table));
    }

    /**
     * Computes the SOR value of a table after augmentation. Based on the symmetric odds ratio but modified to take on
     * low values when the reference +/- read count ratio is skewed but the alt count ratio is not.  Natural log is taken
     * to keep values within roughly the same range as other annotations.
     *
     * Augmentation avoids division by zero.
     *
     * @param originalTable The table before augmentation
     * @return the SOR annotation value
     */
    @VisibleForTesting
    double calculateSOR(final int[][] originalTable) {
        final double[][] augmentedTable = augmentContingencyTable(originalTable);

        double ratio = 0;

        ratio += (augmentedTable[0][0] / augmentedTable[0][1]) * (augmentedTable[1][1] / augmentedTable[1][0]);
        ratio += (augmentedTable[0][1] / augmentedTable[0][0]) * (augmentedTable[1][0] / augmentedTable[1][1]);

        final double refRatio = (Math.min(augmentedTable[0][0], augmentedTable[0][1])/ Math.max(augmentedTable[0][0], augmentedTable[0][1]));
        final double altRatio = (Math.min(augmentedTable[1][0], augmentedTable[1][1])/ Math.max(augmentedTable[1][0], augmentedTable[1][1]));

        return Math.log(ratio) + Math.log(refRatio) - Math.log(altRatio);
    }


    /**
     * Adds the small value AUGMENTATION_CONSTANT to all the entries of the table.
     *
     * @param table the table to augment
     * @return the augmented table
     */
    private static double[][] augmentContingencyTable(final int[][] table) {
        final double[][] augmentedTable = new double[ARRAY_DIM][ARRAY_DIM];
        for ( int i = 0; i < ARRAY_DIM; i++ ) {
            for ( int j = 0; j < ARRAY_DIM; j++ ) {
                augmentedTable[i][j] = table[i][j] + AUGMENTATION_CONSTANT;
            }
        }

        return augmentedTable;
    }

    /**
     * Returns an annotation result given a ratio
     *
     * @param ratio the symmetric odds ratio of the contingency table
     * @return a hash map from SOR
     */
    @VisibleForTesting
    Map<String, Object> annotationForOneTable(final double ratio) {
        final Object value = String.format("%.3f", ratio);
        return Collections.singletonMap(getKeyNames().get(0), value);
    }

    @Override
    public List<String> getKeyNames() {
        return Collections.singletonList(GATKVCFConstants.STRAND_ODDS_RATIO_KEY);
    }

    @Override
    public List<VCFInfoHeaderLine> getDescriptions() {
        return Collections.singletonList(GATKVCFHeaderLines.getInfoLine(getKeyNames().get(0)));
    }
}
