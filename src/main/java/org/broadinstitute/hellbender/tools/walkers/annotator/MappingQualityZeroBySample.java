package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import org.broadinstitute.hellbender.engine.AlignmentContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.GenotypeAnnotation;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.pileup.PileupElement;
import org.broadinstitute.hellbender.utils.pileup.ReadPileup;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;

import java.util.Arrays;
import java.util.List;

/**
 * Count of reads with mapping quality zero for each sample
 *
 * <p>This annotation gives you the count of all reads that have MAPQ = 0 for each sample. The count of reads with MAPQ0 can be used for quality control; high counts typically indicate regions where it is difficult to make confident calls.</p>
 *
 * <h3>Caveat</h3>
 * <p>It is not useful to apply this annotation with HaplotypeCaller because HC filters out all reads with MQ0 upfront, so the annotation will always return a value of 0.</p>
 *
 * <h3>Related annotations</h3>
 * <ul>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_MappingQualityZero.php">MappingQualityZero</a></b> gives the count of reads with MAPQ=0 across all samples.</li>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_LowMQ.php">LowMQ</a></b> gives the proportion of reads with low mapping quality (MAPQ below 10, including 0).</li>
 * </ul>
 */
public class MappingQualityZeroBySample extends GenotypeAnnotation {
    public void annotate(final ReferenceContext ref,
                         final AlignmentContext stratifiedContext,
                         final VariantContext vc,
                         final Genotype g,
                         final GenotypeBuilder gb,
                         final PerReadAlleleLikelihoodMap alleleLikelihoodMap){
        if ( g == null || !g.isCalled() || stratifiedContext == null ) {
            return;
        }

        int mq0 = 0;
        final ReadPileup pileup = stratifiedContext.getBasePileup();
        for (final PileupElement p : pileup ) {
            if ( p.getMappingQual() == 0 ) {
                mq0++;
            }
        }

        gb.attribute(getKeyNames().get(0), mq0);
    }

    public List<String> getKeyNames() { return Arrays.asList(GATKVCFConstants.MAPPING_QUALITY_ZERO_BY_SAMPLE_KEY); }

    public List<VCFFormatHeaderLine> getDescriptions() { return Arrays.asList(GATKVCFHeaderLines.getFormatLine(getKeyNames().get(0))); }


}
