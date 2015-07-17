package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.*;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class InbreedingCoeffUnitTest {
    private static double DELTA_PRECISION = 0.001;
    private Allele Aref, T, C;
    private int[] hetPLs, homRefPLs;

    @BeforeSuite
    public void setup() {
        // alleles
        Aref = Allele.create("A", true);
        T = Allele.create("T");
        C = Allele.create("C");

        // simulating 20 reads with Q30 base qualities
        hetPLs = new int[] {240, 0, 240};
        homRefPLs = new int[] {0, 60, 600};
    }

    private Genotype makeGwithPLs(String sample, Allele a1, Allele a2, double[] pls) {
        Genotype gt = new GenotypeBuilder(sample, Arrays.asList(a1, a2)).PL(pls).make();
        if ( pls != null && pls.length > 0 ) {
            Assert.assertNotNull(gt.getPL());
            Assert.assertTrue(gt.getPL().length > 0);
            for ( int i : gt.getPL() ) {
                Assert.assertTrue(i >= 0);
            }
            Assert.assertNotEquals(Arrays.toString(gt.getPL()), "[0]");
        }
        return gt;
    }

    private Genotype makeG(String sample, Allele a1, Allele a2, int... pls) {
        return new GenotypeBuilder(sample, Arrays.asList(a1, a2)).PL(pls).make();
    }

    private VariantContext makeVC(String source, List<Allele> alleles, Genotype... genotypes) {
        int start = 10;
        int stop = start; // alleles.contains(ATC) ? start + 3 : start;
        return new VariantContextBuilder(source, "1", start, stop, alleles).genotypes(Arrays.asList(genotypes)).filters((Set<String>) null).make();
    }

    @Test
    public void testInbreedingCoeffForMultiallelicVC() {
        //make sure that compound gets (with no ref) don't add to het count
        VariantContext test1 = makeVC("1", Arrays.asList(Aref, T, C),
                makeG("s1",Aref,T,2530,0,7099,366,3056,14931),
                makeG("s2",T,T,7099,2530,0,7099,366,3056,14931),
                makeG("s3",T,C,7099,2530,7099,3056,0,14931),
                makeG("s4",Aref,T,2530,0,7099,366,3056,14931),
                makeG("s5",T,T,7099,2530,0,7099,366,3056,14931),
                makeG("s6",Aref,T,2530,0,7099,366,3056,14931),
                makeG("s7",T,T,7099,2530,0,7099,366,3056,14931),
                makeG("s8",Aref,T,2530,0,7099,366,3056,14931),
                makeG("s9",T,T,7099,2530,0,7099,366,3056,14931),
                makeG("s10",Aref,T,2530,0,7099,366,3056,14931));

        final double ICresult1 = new InbreedingCoeff().calculateIC(test1, test1.getGenotypes());
        Assert.assertEquals(ICresult1, -0.3333333, DELTA_PRECISION, "Pass");

        //make sure that hets with different alternate alleles all get counted
        VariantContext test2 = makeVC("2", Arrays.asList(Aref, T, C),
            makeG("s1",Aref,C,4878,1623,11297,0,7970,8847),
            makeG("s2",Aref,T,2530,0,7099,366,3056,14931),
            makeG("s3",Aref,T,3382,0,6364,1817,5867,12246),
            makeG("s4",Aref,T,2488,0,9110,3131,9374,12505),
            makeG("s5",Aref,C,4530,2006,18875,0,6847,23949),
            makeG("s6",Aref,T,5325,0,18692,389,16014,24570),
            makeG("s7",Aref,T,2936,0,29743,499,21979,38630),
            makeG("s8",Aref,T,6902,0,8976,45,5844,9061),
            makeG("s9",Aref,T,5732,0,10876,6394,11408,17802),
            makeG("s10",Aref,T,2780,0,25045,824,23330,30939));

        final double ICresult2 = new InbreedingCoeff().calculateIC(test2, test2.getGenotypes());
        Assert.assertEquals(ICresult2, -1.0, DELTA_PRECISION, "Pass");
    }

    @Test
    public void testSingletonVsCommonAllele() {

        final List<Genotype> allGTs = new ArrayList<>();
        final int numHomRefGTs = 10000;
        for ( int i = 0; i < numHomRefGTs; i++ )
            allGTs.add(makeG("ref" + i, Aref, Aref, homRefPLs));

        allGTs.add(makeG("het0", Aref, T, hetPLs));
        int numHetGTs = 1;

        final VariantContext singleton = makeVC("singleton", Arrays.asList(Aref, T), allGTs.toArray(new Genotype[allGTs.size()]));
        final double ICsingleton = new InbreedingCoeff().calculateIC(singleton, singleton.getGenotypes());

        final int targetNumHetGTs = 20;
        for ( int i = numHetGTs; i < targetNumHetGTs; i++ )
            allGTs.add(makeG("het" + i, Aref, T, hetPLs));

        final VariantContext common = makeVC("common", Arrays.asList(Aref, T), allGTs.toArray(new Genotype[allGTs.size()]));
        final double ICcommon = new InbreedingCoeff().calculateIC(common, common.getGenotypes());

        Assert.assertTrue(Math.abs(ICsingleton) < Math.abs(ICcommon), String.format("singleton=%f common=%f", ICsingleton, ICcommon));
    }

    @Test
    public void testLargeCohorts() {

        final List<Genotype> allGTs = new ArrayList<>();
        final int numHomRefGTs = 1000000;
        for ( int i = 0; i < numHomRefGTs; i++ )
            allGTs.add(makeG("ref" + i, Aref, Aref, homRefPLs));

        allGTs.add(makeG("het0", Aref, T, hetPLs));
        int numHetGTs = 1;

        final VariantContext singleton = makeVC("singleton", Arrays.asList(Aref, T), allGTs.toArray(new Genotype[allGTs.size()]));
        final double ICsingleton = new InbreedingCoeff().calculateIC(singleton, singleton.getGenotypes());

        for ( int i = numHetGTs; i < 100; i++ ) {
            allGTs.add(makeG("het" + i, Aref, T, hetPLs));
            numHetGTs++;
        }

        final VariantContext hundredton = makeVC("hundredton", Arrays.asList(Aref, T), allGTs.toArray(new Genotype[allGTs.size()]));
        final double IChundredton = new InbreedingCoeff().calculateIC(hundredton, hundredton.getGenotypes());

        Assert.assertTrue(Math.abs(ICsingleton) < Math.abs(IChundredton), String.format("singleton=%f hundredton=%f", ICsingleton, IChundredton));

        for ( int i = numHetGTs; i < numHomRefGTs; i++ )
            allGTs.add(makeG("het" + i, Aref, T, hetPLs));

        final VariantContext common = makeVC("common", Arrays.asList(Aref, T), allGTs.toArray(new Genotype[allGTs.size()]));
        final double ICcommon = new InbreedingCoeff().calculateIC(common, common.getGenotypes());

        Assert.assertTrue(Math.abs(IChundredton) < Math.abs(ICcommon), String.format("hundredton=%f common=%f", IChundredton, ICcommon));
    }

    @Test
    public void testAllHetsForLargeCohorts() {

        final int numGTs = 1000000;

        final List<Genotype> singletonGTs = new ArrayList<>();
        for ( int i = 0; i < numGTs; i++ )
            singletonGTs.add(makeG("ref" + i, Aref, Aref, homRefPLs));

        singletonGTs.add(makeG("het0", Aref, T, hetPLs));

        final VariantContext singleton = makeVC("singleton", Arrays.asList(Aref, T), singletonGTs.toArray(new Genotype[singletonGTs.size()]));
        final double ICsingleton = new InbreedingCoeff().calculateIC(singleton, singleton.getGenotypes());

        final List<Genotype> allHetGTs = new ArrayList<>();
        for ( int i = 0; i < numGTs; i++ )
            allHetGTs.add(makeG("het" + i, Aref, T, hetPLs));

        final VariantContext allHet = makeVC("allHet", Arrays.asList(Aref, T), allHetGTs.toArray(new Genotype[allHetGTs.size()]));
        final double ICHets = new InbreedingCoeff().calculateIC(allHet, allHet.getGenotypes());

        Assert.assertTrue(Math.abs(ICsingleton) < Math.abs(ICHets), String.format("singleton=%f allHets=%f", ICsingleton, ICHets));
    }
}
