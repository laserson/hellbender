package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.AnnotatorCompatible;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.InfoFieldAnnotation;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.pileup.PileupElement;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Proportion of low quality reads
 *
 * <p>This annotation tells you what fraction of reads have a mapping quality of less than the given threshold of 10 (including 0). Note that certain tools may impose a different minimum mapping quality threshold. For example, HaplotypeCaller excludes reads with MAPQ<20.</p>
 *
 * <h3>Calculation</h3>
 * $$ LowMQ = \frac{# reads with MAPQ=0 + # reads with MAPQ<10}{total # reads} $$
 *
 * <h3>Related annotations</h3>
 * <ul>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_MappingQualityZero.php">MappingQualityZero</a></b> gives the count of reads with MAPQ=0 across all samples.</li>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_MappingQualityZeroBySample.php">MappingQualityZeroBySample</a></b> gives the count of reads with MAPQ=0 for each individual sample.</li>
 * </ul>
 */
public class LowMQ extends InfoFieldAnnotation {

    public Map<String, Object> annotate(final RefMetaDataTracker tracker,
                                        final AnnotatorCompatible walker,
                                        final ReferenceContext ref,
                                        final Map<String, AlignmentContext> stratifiedContexts,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {
        if ( stratifiedContexts.size() == 0 ) {
            return null;
        }

        double mq0 = 0;
		double mq10 = 0;
		double total = 0;
        for ( final Map.Entry<String, AlignmentContext> sample : stratifiedContexts.entrySet() )
		{
            for ( final PileupElement p : sample.getValue().getBasePileup() )
			{
                if ( p.getMappingQual() == 0 )  { mq0 += 1; }
                if ( p.getMappingQual() <= 10 ) { mq10 += 1; }
				total += 1; 
            }
        }
        final Map<String, Object> map = new HashMap<>();
        map.put(getKeyNames().get(0), String.format("%.04f,%.04f,%.00f", mq0 / total, mq10 / total, total));
        return map;
    }

    public List<String> getKeyNames() { return Arrays.asList(GATKVCFConstants.LOW_MQ_KEY); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(GATKVCFHeaderLines.getInfoLine(getKeyNames().get(0))); }
}
