package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.AnnotatorCompatible;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.InfoFieldAnnotation;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;

import java.util.*;

/**
 * General category of variant
 *
 * <p>This annotation assigns a roughly correct category of the variant type (SNP, MNP, insertion, deletion, etc.). It also specifies whether the variant is multiallelic (>2 alleles).</p>
 */
public class VariantType extends InfoFieldAnnotation {

    public Map<String, Object> annotate(final RefMetaDataTracker tracker,
                                        final AnnotatorCompatible walker,
                                        final ReferenceContext ref,
                                        final Map<String, AlignmentContext> stratifiedContexts,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> stratifiedPerReadAlleleLikelihoodMap) {

        final StringBuffer type = new StringBuffer("");
        if ( vc.isVariant() && !vc.isBiallelic() ) {
            type.append("MULTIALLELIC_");
        }

        if ( !vc.isIndel() ) {
            type.append(vc.getType().toString());
        } else {
            if (vc.isSimpleInsertion()) {
                type.append("INSERTION.");
            } else if (vc.isSimpleDeletion()) {
                type.append("DELETION.");
            } else {
                type.append("COMPLEX.");
            }
            final ArrayList<Integer> inds = IndelUtils.findEventClassificationIndex(vc, ref);
            type.append(IndelUtils.getIndelClassificationName(inds.get(0)));

            for (int i = 1; i < inds.size(); i++ ) {
                type.append(".");
                type.append(IndelUtils.getIndelClassificationName(inds.get(i)));
            }
        }

        final Map<String, Object> map = new HashMap<>();
        map.put(getKeyNames().get(0), String.format("%s", type));
        return map;
    }

    public List<String> getKeyNames() { return Arrays.asList(GATKVCFConstants.VARIANT_TYPE_KEY); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(GATKVCFHeaderLines.getInfoLine(getKeyNames().get(0))); }

}
