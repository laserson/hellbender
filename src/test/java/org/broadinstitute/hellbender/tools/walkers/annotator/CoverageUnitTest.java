package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.samtools.CigarElement;
import htsjdk.samtools.TextCigarCodec;
import htsjdk.samtools.util.Locatable;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFConstants;
import org.broadinstitute.hellbender.engine.AlignmentContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.InfoFieldAnnotation;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;
import org.broadinstitute.hellbender.utils.pileup.PileupElement;
import org.broadinstitute.hellbender.utils.pileup.ReadPileup;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class CoverageUnitTest extends BaseTest {
    @Test
    public void testAllNull() throws Exception {
        final Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap = null;
        final Map<String, AlignmentContext> stratifiedContexts= null;
        final VariantContext vc= null;
        final ReferenceContext referenceContext= null;
        final InfoFieldAnnotation cov = new Coverage();
        final Map<String, Object> annotate = cov.annotate(referenceContext, stratifiedContexts, vc, perReadAlleleLikelihoodMap);
        Assert.assertNull(annotate);

        Assert.assertEquals(cov.getDescriptions().size(), 1);
        Assert.assertEquals(cov.getDescriptions().get(0).getID(), VCFConstants.DEPTH_KEY);
    }

    @Test
    public void testStratifiedContextsEmpty() throws Exception {
        final Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap = null;
        final Map<String, AlignmentContext> stratifiedContexts= Collections.emptyMap();
        final VariantContext vc= null;
        final ReferenceContext referenceContext= null;
        final Map<String, Object> annotate = new Coverage().annotate(referenceContext, stratifiedContexts, vc, perReadAlleleLikelihoodMap);
        Assert.assertNull(annotate);
    }

    @Test
    public void testPerReadAlleleLikelihoodMapEmpty() throws Exception {
        final Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap = Collections.emptyMap();
        final Map<String, AlignmentContext> stratifiedContexts= null;
        final VariantContext vc= null;
        final ReferenceContext referenceContext= null;
        final Map<String, Object> annotate = new Coverage().annotate(referenceContext, stratifiedContexts, vc, perReadAlleleLikelihoodMap);
        Assert.assertNull(annotate);
    }

    @Test
    public void testStratifiedContexts() throws Exception {
        final Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap = null;
        final Locatable loc= new SimpleInterval("1",1,1);
        final List<PileupElement> elements= new ArrayList<>();

        final int n = 15;
        for (int i = 0; i < n; i++) {
            final GATKRead read= ArtificialReadUtils.createArtificialRead(TextCigarCodec.decode("10M"));
            final int baseOffset= 0;
            final CigarElement currentElement= read.getCigar().getCigarElement(0);
            final int currentCigarOffset= 0;
            final int offsetInCurrentCigar= 0;
            final PileupElement el= new PileupElement(read, baseOffset, currentElement, currentCigarOffset, offsetInCurrentCigar);
            elements.add(el);
        }
        final ReadPileup pileup= new ReadPileup(loc, elements);
        final AlignmentContext ac= new AlignmentContext(loc, pileup);
        final Map<String, AlignmentContext> stratifiedContexts= Collections.singletonMap("sample1", ac);
        final VariantContext vc= null;
        final ReferenceContext referenceContext= null;
        final Map<String, Object> annotate = new Coverage().annotate(referenceContext, stratifiedContexts, vc, perReadAlleleLikelihoodMap);
        Assert.assertEquals(annotate.size(), 1, "size");
        Assert.assertEquals(annotate.keySet(), Collections.singleton(VCFConstants.DEPTH_KEY), "annots");
        Assert.assertEquals(annotate.get(VCFConstants.DEPTH_KEY), String.valueOf(n));
    }

    @Test
    public void testPerReadAlleleLikelihoodMap(){
        final PerReadAlleleLikelihoodMap map= new PerReadAlleleLikelihoodMap();

        final Allele alleleT = Allele.create("T");
        final Allele alleleA = Allele.create("A");
        final double lik= -1.0;

        final int n1A= 3;
        final int n1T= 5;
        for (int i = 0; i < n1A; i++) {
            map.add(ArtificialReadUtils.createArtificialRead(TextCigarCodec.decode("10M")), alleleA, lik);
        }
        for (int i = 0; i < n1T; i++) {
            //try to fool it - add 2 alleles for same read
            final GATKRead read = ArtificialReadUtils.createArtificialRead(TextCigarCodec.decode("10M"));
            map.add(read, alleleA, lik);
            map.add(read, alleleT, lik);
        }

        final Map<String, PerReadAlleleLikelihoodMap> perReadAlleleLikelihoodMap = Collections.singletonMap("sample1", map);
        final Map<String, AlignmentContext> stratifiedContexts= null;
        final VariantContext vc= null;
        final ReferenceContext referenceContext= null;
        final Map<String, Object> annotate = new Coverage().annotate(referenceContext, stratifiedContexts, vc, perReadAlleleLikelihoodMap);
        Assert.assertEquals(annotate.size(), 1, "size");
        Assert.assertEquals(annotate.keySet(), Collections.singleton(VCFConstants.DEPTH_KEY), "annots");
        final int n= n1A + n1T;
        Assert.assertEquals(annotate.get(VCFConstants.DEPTH_KEY), String.valueOf(n));

    }
}
