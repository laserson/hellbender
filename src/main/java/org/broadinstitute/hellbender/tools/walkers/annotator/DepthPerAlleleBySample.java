package org.broadinstitute.hellbender.tools.walkers.annotator;


import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFStandardHeaderLines;
import org.broadinstitute.hellbender.engine.AlignmentContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.GenotypeAnnotation;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.StandardAnnotation;
import org.broadinstitute.hellbender.utils.genotyper.MostLikelyAllele;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.pileup.PileupElement;
import org.broadinstitute.hellbender.utils.pileup.ReadPileup;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.*;

/**
 * Depth of coverage of each allele per sample
 *
 * <p>Also known as the allele depth, this annotation gives the unfiltered count of reads that support a given allele for an individual sample. The values in the field are ordered to match the order of alleles specified in the REF and ALT fields: REF, ALT1, ALT2 and so on if there are multiple ALT alleles.</p>
 *
 * <p>See the method documentation on <a href="http://www.broadinstitute.org/gatk/guide/article?id=4721">using coverage information</a> for important interpretation details.</p>
 *
 * <h3>Caveats</h3>
 * <ul>
 *     <li>The AD calculation as performed by HaplotypeCaller may not yield exact results because only reads that statistically favor one allele over the other are counted. Due to this fact, the sum of AD may be different than the individual sample depth, especially when there are many non-informative reads.</li>
 *     <li>For the AD calculation as performed by the UnifiedGenotyper, the same caveat as above applies to indels (but not to SNPs).</li>
 *     <li>Because the AD includes reads and bases that were filtered by the caller (and in case of indels, is based on a statistical computation), it  should not be used to make assumptions about the genotype that it is associated with. Ultimately, the phred-scaled genotype likelihoods (PLs) are what determines the genotype calls.</li>
 * </ul>
 *
 * <h3>Related annotations</h3>
 * <ul>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_Coverage.php">Coverage</a></b> gives the filtered depth of coverage for each sample and the unfiltered depth across all samples.</li>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_AlleleBalance.php">AlleleBalance</a></b> is a generalization of this annotation over all samples.</li>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_AlleleBalanceBySample.php">AlleleBalanceBySample</a></b> calculates allele balance for each individual sample.</li>
 * </ul>
 */
public class DepthPerAlleleBySample extends GenotypeAnnotation implements StandardAnnotation {

    public void annotate(final ReferenceContext ref,
                         final AlignmentContext stratifiedContext,
                         final VariantContext vc,
                         final Genotype g,
                         final GenotypeBuilder gb,
                         final PerReadAlleleLikelihoodMap alleleLikelihoodMap) {
        if ( g == null || !g.isCalled() || ( stratifiedContext == null && alleleLikelihoodMap == null) ) {
            return;
        }

        if (alleleLikelihoodMap != null && !alleleLikelihoodMap.isEmpty()) {
            annotateWithLikelihoods(alleleLikelihoodMap, vc, gb);
        } else if ( stratifiedContext != null && (vc.isSNP())) {
            annotateWithPileup(stratifiedContext, vc, gb);
        }
    }

    private void annotateWithPileup(final AlignmentContext stratifiedContext, final VariantContext vc, final GenotypeBuilder gb) {

        final HashMap<Byte, Integer> alleleCounts = new HashMap<>();
        for ( final Allele allele : vc.getAlleles() ) {
            alleleCounts.put(allele.getBases()[0], 0);
        }

        final ReadPileup pileup = stratifiedContext.getBasePileup();
        for ( final PileupElement p : pileup ) {
            if ( alleleCounts.containsKey(p.getBase()) ) {
                alleleCounts.put(p.getBase(), alleleCounts.get(p.getBase()) + 1);
            }
        }

        // we need to add counts in the correct order
        final int[] counts = new int[alleleCounts.size()];
        counts[0] = alleleCounts.get(vc.getReference().getBases()[0]);
        for (int i = 0; i < vc.getAlternateAlleles().size(); i++) {
            counts[i + 1] = alleleCounts.get(vc.getAlternateAllele(i).getBases()[0]);
        }

        gb.AD(counts);
    }

    private void annotateWithLikelihoods(final PerReadAlleleLikelihoodMap perReadAlleleLikelihoodMap, final VariantContext vc, final GenotypeBuilder gb) {
        final Set<Allele> alleles = new HashSet<>(vc.getAlleles());

        // make sure that there's a meaningful relationship between the alleles in the perReadAlleleLikelihoodMap and our VariantContext
        if ( ! perReadAlleleLikelihoodMap.getAllelesSet().containsAll(alleles) ) {
            throw new IllegalStateException("VC alleles " + alleles + " not a strict subset of per read allele map alleles " + perReadAlleleLikelihoodMap.getAllelesSet());
        }

        final HashMap<Allele, Integer> alleleCounts = new HashMap<>();
        for ( final Allele allele : vc.getAlleles() ) { alleleCounts.put(allele, 0); }

        for ( final Map.Entry<GATKRead,Map<Allele,Double>> el : perReadAlleleLikelihoodMap.getLikelihoodReadMap().entrySet()) {
            final MostLikelyAllele a = PerReadAlleleLikelihoodMap.getMostLikelyAllele(el.getValue(), alleles);
            if (! a.isInformative() ) {
                continue; // read is non-informative
            }
            final GATKRead read = el.getKey();
            final int prevCount = alleleCounts.get(a.getMostLikelyAllele());
            alleleCounts.put(a.getMostLikelyAllele(), prevCount + 1);
        }

        final int[] counts = new int[alleleCounts.size()];
        counts[0] = alleleCounts.get(vc.getReference());
        for (int i = 0; i < vc.getAlternateAlleles().size(); i++) {
            counts[i + 1] = alleleCounts.get(vc.getAlternateAllele(i));
        }

        gb.AD(counts);
    }

    public List<String> getKeyNames() { return Arrays.asList(VCFConstants.GENOTYPE_ALLELE_DEPTHS); }

    public List<VCFFormatHeaderLine> getDescriptions() {
        return Arrays.asList(VCFStandardHeaderLines.getFormatLine(getKeyNames().get(0)));
    }
}