package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.broadinstitute.hellbender.engine.AlignmentContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.AnnotatorCompatible;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.InfoFieldAnnotation;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * List samples that are non-reference at a given site
 *
 * <p>The output is a list of the samples that are genotyped as having one or more variant alleles. This allows you to easily determine which samples are non-reference (heterozygous or homozygous-variant) and compare them to samples that are homozygous-reference.</p>
 */
public class SampleList extends InfoFieldAnnotation {

    public Map<String, Object> annotate(final AnnotatorCompatible walker,
                                        final ReferenceContext ref,
                                        final Map<String, AlignmentContext> stratifiedContexts,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {
        if ( vc.isMonomorphicInSamples() || !vc.hasGenotypes() ) {
            return null;
        }

        final StringBuilder samples = new StringBuilder();
        for ( final Genotype genotype : vc.getGenotypesOrderedByName() ) {
            if ( genotype.isCalled() && !genotype.isHomRef() ){
                if ( samples.length() > 0 ) {
                    samples.append(",");
                }
                samples.append(genotype.getSampleName());
            }
        }

        if ( samples.length() == 0 ) {
            return null;
        }

        final Map<String, Object> map = new HashMap<>();
        map.put(getKeyNames().get(0), samples.toString());
        return map;
    }

    public List<String> getKeyNames() { return Arrays.asList(GATKVCFConstants.SAMPLE_LIST_KEY); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(GATKVCFHeaderLines.getInfoLine(getKeyNames().get(0))); }
}
