package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.*;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

public class QualByDepthUnitTest extends BaseTest {

    @DataProvider(name = "UsingAD")
    public Object[][] makeUsingADData() {
        List<Object[]> tests = new ArrayList<>();

        final Allele A = Allele.create("A", true);
        final Allele C = Allele.create("C");
        final Allele G = Allele.create("G");

        final List<Allele> AA = Arrays.asList(A, A);
        final List<Allele> AC = Arrays.asList(A, C);
        final List<Allele> GG = Arrays.asList(G, G);
        final List<Allele> ACG = Arrays.asList(A, C, G);

        final Genotype gAC = new GenotypeBuilder("1", AC).DP(10).AD(new int[]{5,5}).make();
        final Genotype gAA = new GenotypeBuilder("2", AA).DP(10).AD(new int[]{10,0}).make();
        final Genotype gACerror = new GenotypeBuilder("3", AC).DP(10).AD(new int[]{9,1}).make();
        final Genotype gGG = new GenotypeBuilder("4", GG).DP(10).AD(new int[]{1,9}).make();

        tests.add(new Object[]{new VariantContextBuilder("test", "20", 10, 10, AC).log10PError(-5).genotypes(Arrays.asList(gAC)).make(), 5.0});
        tests.add(new Object[]{new VariantContextBuilder("test", "20", 10, 10, AC).log10PError(-5).genotypes(Arrays.asList(gACerror)).make(), 5.0});
        tests.add(new Object[]{new VariantContextBuilder("test", "20", 10, 10, AC).log10PError(-5).genotypes(Arrays.asList(gAA, gAC)).make(), 5.0});
        tests.add(new Object[]{new VariantContextBuilder("test", "20", 10, 10, AC).log10PError(-5).genotypes(Arrays.asList(gAC, gACerror)).make(), 5.0});
        tests.add(new Object[]{new VariantContextBuilder("test", "20", 10, 10, ACG).log10PError(-5).genotypes(Arrays.asList(gAA, gAC, gACerror, gGG)).make(), 2.5});

        return tests.toArray(new Object[][]{});
    }

    @Test(dataProvider = "UsingAD")
    public void testUsingAD(final VariantContext vc, final double expectedQD) {
        final Map<String, Object> annotatedMap = new QualByDepth().annotate(null, vc, null);
        Assert.assertNotNull(annotatedMap, vc.toString());
        final String QD = (String)annotatedMap.get("QD");
        Assert.assertEquals(Double.valueOf(QD), expectedQD, 0.0001);

        Assert.assertEquals(new QualByDepth().getKeyNames(), Collections.singletonList(GATKVCFConstants.QUAL_BY_DEPTH_KEY));
        Assert.assertEquals(new QualByDepth().getDescriptions(), Collections.singletonList(GATKVCFHeaderLines.getInfoLine(GATKVCFConstants.QUAL_BY_DEPTH_KEY)));

    }

}
