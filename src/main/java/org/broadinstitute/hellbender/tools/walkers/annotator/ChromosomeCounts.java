package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextUtils;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.broadinstitute.hellbender.engine.AlignmentContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.ActiveRegionBasedAnnotation;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.InfoFieldAnnotation;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.StandardAnnotation;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.variant.ChromosomeCountConstants;

import java.util.*;


/**
 * Counts and frequency of alleles in called genotypes
 *
 * <p>This annotation outputs the following:</p>
 *
 *     <ul>
 *     <li>Number of times each ALT allele is represented, in the same order as listed (AC)</li>
 *     <li>Frequency of each ALT allele, in the same order as listed (AF)</li>
 *     <li>Total number of alleles in called genotypes (AN)</li>
 * </ul>
 * <h3>Example</h3>
 * <pre>AC=1;AF=0.500;AN=2</pre>
 * <p>This set of annotations, relating to a heterozygous call(0/1) means there is 1 alternate allele in the genotype. The corresponding allele frequency is 0.5 because there is 1 alternate allele and 1 reference allele in the genotype.
 * The total number of alleles in the genotype should be equivalent to the ploidy of the sample.</p>
 *
 */
public class ChromosomeCounts extends InfoFieldAnnotation implements StandardAnnotation, ActiveRegionBasedAnnotation {

    private Set<String> founderIds = new HashSet<>();

    ChromosomeCounts(final Set<String> founderIds){
        //If families were given, get the founders ids
        this.founderIds = founderIds;
    }

    ChromosomeCounts(){
        this(null);
    }

    public Map<String, Object> annotate(final ReferenceContext ref,
                                        final Map<String, AlignmentContext> stratifiedContexts,
                                        final VariantContext vc,
                                        final Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap) {
        if ( ! vc.hasGenotypes() ) {
            return null;
        }

        return VariantContextUtils.calculateChromosomeCounts(vc, new HashMap<>(), true, founderIds);
    }

    public List<String> getKeyNames() {
        return Arrays.asList(ChromosomeCountConstants.keyNames);
    }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(ChromosomeCountConstants.descriptions); }
}