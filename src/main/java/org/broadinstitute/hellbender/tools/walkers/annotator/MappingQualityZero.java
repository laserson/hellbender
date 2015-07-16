package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import htsjdk.variant.vcf.VCFStandardHeaderLines;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.ActiveRegionBasedAnnotation;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.AnnotatorCompatible;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.InfoFieldAnnotation;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.pileup.PileupElement;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Count of all reads with MAPQ = 0 across all samples
 *
 * <p>This anotation gives you the count of all reads that have MAPQ = 0 across all samples. The count of reads with MAPQ0 can be used for quality control; high counts typically indicate regions where it is difficult to make confident calls.</p>
 *
 * <h3>Caveat</h3>
 * <p>It is not useful to apply this annotation with HaplotypeCaller because HC filters out all reads with MQ0 upfront, so the annotation will always return a value of 0.</p>
 *
 * <h3>Related annotations</h3>
 * <ul>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_MappingQualityZeroBySample.php">MappingQualityZeroBySample</a></b> gives the count of reads with MAPQ=0 for each individual sample.</li>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_LowMQ.php">LowMQ</a></b> gives the proportion of reads with low mapping quality (MAPQ below 10, including 0).</li>
 * </ul>
 *
 */
public class MappingQualityZero extends InfoFieldAnnotation implements ActiveRegionBasedAnnotation {

    public Map<String, Object> annotate(final RefMetaDataTracker tracker,
                                        final AnnotatorCompatible walker,
                                        final ReferenceContext ref,
                                        final Map<String, AlignmentContext> stratifiedContexts,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {
        if ((vc.isSNP() || !vc.isVariant()) && stratifiedContexts != null) {
            return annotatePileup(ref, stratifiedContexts, vc);
        } else if (stratifiedPerReadAlleleLikelihoodMap != null && vc.isVariant()) {
            return annotateWithLikelihoods(stratifiedPerReadAlleleLikelihoodMap, vc);
        } else {
            return null;
        }
    }

    private Map<String, Object> annotatePileup(final ReferenceContext ref,
                                               final Map<String, AlignmentContext> stratifiedContexts,
                                               final VariantContext vc) {
        if ( stratifiedContexts.isEmpty() ) {
            return null;
        }

        int mq0 = 0;
        for ( final Map.Entry<String, AlignmentContext> sample : stratifiedContexts.entrySet() ) {
            final AlignmentContext context = sample.getValue();
            final ReadBackedPileup pileup = context.getBasePileup();
            for (final PileupElement p : pileup ) {
                if ( p.getMappingQual() == 0 ) {
                    mq0++;
                }
            }
        }
        final Map<String, Object> map = new HashMap<>();
        map.put(getKeyNames().get(0), String.format("%d", mq0));
        return map;
    }

    private Map<String, Object> annotateWithLikelihoods(final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap,
                                                        final VariantContext vc) {
        if ( stratifiedPerReadAlleleLikelihoodMap == null ) {
            return null;
        }

        int mq0 = 0;
        for ( final PerReadAlleleLikelihoodMap likelihoodMap : stratifiedPerReadAlleleLikelihoodMap.values() ) {
            for (final GATKRead read : likelihoodMap.getLikelihoodReadMap().keySet()) {

                 if (read.getMappingQuality() == 0 ) {
                     mq0++;
                 }
            }
        }
        final Map<String, Object> map = new HashMap<String, Object>();
        map.put(getKeyNames().get(0), String.format("%d", mq0));
        return map;
    }


    public List<String> getKeyNames() { return Arrays.asList(VCFConstants.MAPPING_QUALITY_ZERO_KEY); }

    public List<VCFInfoHeaderLine> getDescriptions() {
        return Arrays.asList(VCFStandardHeaderLines.getInfoLine(getKeyNames().get(0)));
    }
}