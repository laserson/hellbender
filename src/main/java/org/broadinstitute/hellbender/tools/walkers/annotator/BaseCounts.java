package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.AnnotatorCompatible;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.InfoFieldAnnotation;
import org.broadinstitute.hellbender.utils.BaseUtils;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Count of A, C, G, T bases across all samples
 *
 * <p> This annotation returns the counts of A, C, G, and T bases across all samples, in that order.</p>
 * <h3>Example:</h3>
 *
 * <pre>BaseCounts=3,0,3,0</pre>
 *
 * <p>
 *     This means the number of A bases seen is 3, the number of T bases seen is 0, the number of G bases seen is 3, and the number of T bases seen is 0.
 * </p>
 *
 * <h3>Related annotations</h3>
 * <ul>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_NBaseCount.php">NBaseCount</a></b> counts the percentage of N bases.</li>
 * </ul>
 */

 public class BaseCounts extends InfoFieldAnnotation {

    public Map<String, Object> annotate(final RefMetaDataTracker tracker,
                                        final AnnotatorCompatible walker,
                                        final ReferenceContext ref,
                                        final Map<String, AlignmentContext> stratifiedContexts,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {
        if ( stratifiedContexts.size() == 0 ) {
            return null;
        }

        final int[] counts = new int[4];

        for ( final Map.Entry<String, AlignmentContext> sample : stratifiedContexts.entrySet() ) {
            for (final byte base : sample.getValue().getBasePileup().getBases() ) {
                final int index = BaseUtils.simpleBaseToBaseIndex(base);
                if ( index != -1 ) {
                    counts[index]++;
                }
            }
        }
        final Map<String, Object> map = new HashMap<>();
        map.put(getKeyNames().get(0), counts);
        return map;
    }

    public List<String> getKeyNames() { return Arrays.asList(GATKVCFConstants.BASE_COUNTS_KEY); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(GATKVCFHeaderLines.getInfoLine(getKeyNames().get(0))); }
}