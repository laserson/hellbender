package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.broadinstitute.hellbender.engine.AlignmentContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
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
 * GC content of the reference around the given site
 *
 * <p>The GC content is the number of GC bases relative to the total number of bases (# GC bases / # all bases) around this site on the reference. Some sequencing technologies have trouble with high GC content because of the stronger bonds of G-C nucleotide pairs, so high GC values tend to be associated with low coverage depth and lower confidence calls.</p>
 *
 * <h3>Caveat</h3>
 * <p>The window size used to calculate the GC content around the site is determined by the tool used for annotation (UnifiedGenotyper, HaplotypeCaller or VariantAnnotator). See the <a href="https://www.broadinstitute.org/gatk/guide/tooldocs/">Tool Documentation</a> for each of these tools to find out what window size they use.</p>
 */
public class GCContent extends InfoFieldAnnotation {

    public Map<String, Object> annotate(final ReferenceContext ref,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {
        final double content = computeGCContent(ref);
        final Map<String, Object> map = new HashMap<>();
        map.put(getKeyNames().get(0), String.format("%.2f", content));
        return map;
    }

    public List<String> getKeyNames() { return Arrays.asList(GATKVCFConstants.GC_CONTENT_KEY); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(GATKVCFHeaderLines.getInfoLine(getKeyNames().get(0)));}

    public boolean useZeroQualityReads() { return false; }

    private static double computeGCContent(final ReferenceContext ref) {
        int gc = 0, at = 0;

        for ( final byte base : ref.getBases() ) {
            final int baseIndex = BaseUtils.simpleBaseToBaseIndex(base);
            if ( baseIndex == BaseUtils.Base.G.ordinal() || baseIndex == BaseUtils.Base.C.ordinal() ) {
                gc++;
            } else if ( baseIndex == BaseUtils.Base.A.ordinal() || baseIndex == BaseUtils.Base.T.ordinal() ) {
                at++;
            } else {
                ; // ignore
            }
        }

        final int sum = gc + at;
        return (100.0*gc) / (sum == 0 ? 1 : sum);
     }
}