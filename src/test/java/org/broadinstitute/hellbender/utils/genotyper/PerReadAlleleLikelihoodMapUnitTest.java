package org.broadinstitute.hellbender.utils.genotyper;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.util.Locatable;
import htsjdk.variant.variantcontext.Allele;
import joptsimple.internal.Strings;
import org.broadinstitute.hellbender.utils.*;
import org.broadinstitute.hellbender.utils.pileup.PileupElement;
import org.broadinstitute.hellbender.utils.pileup.ReadPileup;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

public final class PerReadAlleleLikelihoodMapUnitTest extends BaseTest {

    @Test()
    public void testMultiAlleleWithHomLiks() {
        final ReadPileup pileup = makeArtificialPileup();

        Allele base_A = Allele.create(BaseUtils.Base.A.base);
        Allele base_C = Allele.create(BaseUtils.Base.C.base);
        Allele base_T = Allele.create(BaseUtils.Base.T.base);

        final List<Allele> allAlleles = Arrays.asList(base_A, base_C, base_T);
        final double likA = -0.04;
        final double likNotA = -3.0;
        PerReadAlleleLikelihoodMap perReadAlleleLikelihoodMap = new PerReadAlleleLikelihoodMap();
        for ( final PileupElement e : pileup ) {
            for ( final Allele allele : allAlleles ) {
                Double likelihood = allele == base_A ? likA : likNotA;
                perReadAlleleLikelihoodMap.add(e, allele, likelihood);
            }
        }

        Assert.assertEquals(perReadAlleleLikelihoodMap.getAllelesSet(), new HashSet<>(allAlleles));
        Assert.assertEquals(perReadAlleleLikelihoodMap.size(), pileup.size());
        Assert.assertEquals(perReadAlleleLikelihoodMap.getAlleleStratifiedReadMap().keySet().size(), 3);
        Map<Allele,List<GATKRead>> shouldBeAllA = perReadAlleleLikelihoodMap.getAlleleStratifiedReadMap();
        Assert.assertEquals(shouldBeAllA.get(base_A).size(), pileup.size());
        Assert.assertEquals(shouldBeAllA.get(base_C).size(), 0);
        Assert.assertEquals(shouldBeAllA.get(base_T).size(), 0);

        final Map<GATKRead, Map<Allele, Double>> readMap = perReadAlleleLikelihoodMap.getLikelihoodReadMap();
        Assert.assertEquals(readMap.size(), pileup.size());

        final Map<Allele, Double> likMap = perReadAlleleLikelihoodMap.getLikelihoods(pileup.get(0));
        Assert.assertEquals(likMap.size(), 3); //three allele
        Assert.assertEquals(likMap.get(base_A), likA);
        Assert.assertEquals(likMap.get(base_C), likNotA);

        ReadPileup newPileup = makeArtificialPileup();
        Assert.assertNull(perReadAlleleLikelihoodMap.getLikelihoods(newPileup.get(0)));

        perReadAlleleLikelihoodMap.toString(); //checking blowup, no asserts

        perReadAlleleLikelihoodMap.clear();
        Assert.assertEquals(perReadAlleleLikelihoodMap.size(), 0);

        perReadAlleleLikelihoodMap.toString(); //checking blowup, no asserts
    }

    private ReadPileup makeArtificialPileup() {
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeader();
        final Locatable myLocation = new SimpleInterval("1", 10, 10);

        final int pileupSize = 100;
        final int readLength = 10;
        final List<GATKRead> reads = new LinkedList<>();
        for ( int i = 0; i < pileupSize; i++ ) {
            final GATKRead read = ArtificialReadUtils.createArtificialRead(header, "myRead" + i, 0, 1, readLength);
            final byte[] bases = Utils.dupBytes((byte) 'A', readLength);
            bases[0] = (byte)(i % 2 == 0 ? 'A' : 'C'); // every other read the first base is a C

            // set the read's bases and quals
            read.setBases(bases);
            read.setBaseQualities(Utils.dupBytes((byte)30, readLength));
            reads.add(read);
        }

        // create a pileup with all reads having offset 0
        return new ReadPileup(myLocation, reads, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testBadLikelihood(){
        final ReadPileup pileup = makeArtificialPileup();
        PerReadAlleleLikelihoodMap perReadAlleleLikelihoodMap = new PerReadAlleleLikelihoodMap();
        Allele base_A = Allele.create(BaseUtils.Base.A.base);
        final Allele allele = base_A;
        PileupElement e = pileup.get(0);
        perReadAlleleLikelihoodMap.add(e,allele, 0.1);  //boom: positive likelihood
    }

    @Test()
    public void testMultiAlleleWithHetLiks() {
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeader();
        final Locatable myLocation = new SimpleInterval("1", 10, 10);

        final int pileupSize = 100;
        final int readLength = 10;
        final List<GATKRead> reads = new LinkedList<>();
        for ( int i = 0; i < pileupSize; i++ ) {
            final GATKRead read = ArtificialReadUtils.createArtificialRead(header, "myRead" + i, 0, 1, readLength);
            final byte[] bases = Utils.dupBytes((byte)'A', readLength);
            bases[0] = (byte)(i % 2 == 0 ? 'A' : 'C'); // every other base is a C

            // set the read's bases and quals
            read.setBases(bases);
            read.setBaseQualities(Utils.dupBytes((byte)30, readLength));
            reads.add(read);
        }

        // create a pileup with all reads having offset 0
        final ReadPileup pileup = new ReadPileup(myLocation, reads, 0);
        Allele base_A = Allele.create(BaseUtils.Base.A.base);
        Allele base_C = Allele.create(BaseUtils.Base.C.base);
        Allele base_T = Allele.create(BaseUtils.Base.T.base);

        PerReadAlleleLikelihoodMap perReadAlleleLikelihoodMap = new PerReadAlleleLikelihoodMap();
        int idx = 0;
        for ( final PileupElement e : pileup ) {
            for ( final Allele allele : Arrays.asList(base_A, base_C, base_T) ) {
                Double likelihood;
                if ( idx % 2 == 0 )
                    likelihood = allele == base_A ? -0.04 : -3.0;
                else
                    likelihood = allele == base_C ? -0.04 : -3.0;
                perReadAlleleLikelihoodMap.add(e,allele,likelihood);
            }
            idx++;
        }

        Assert.assertEquals(perReadAlleleLikelihoodMap.size(), pileup.size());
        Assert.assertEquals(perReadAlleleLikelihoodMap.getAlleleStratifiedReadMap().keySet().size(), 3);
        Map<Allele,List<GATKRead>> halfAhalfC = perReadAlleleLikelihoodMap.getAlleleStratifiedReadMap();
        Assert.assertEquals(halfAhalfC.get(base_A).size(), pileup.size() / 2);
        Assert.assertEquals(halfAhalfC.get(base_C).size(), pileup.size() / 2);
        Assert.assertEquals(halfAhalfC.get(base_T).size(), 0);

        // make sure the likelihoods are retrievable

        idx = 0;
        for ( final PileupElement e : pileup ) {
            Assert.assertTrue(perReadAlleleLikelihoodMap.containsPileupElement(e));
            Map<Allele,Double> likelihoods = perReadAlleleLikelihoodMap.getLikelihoods(e);
            for ( final Allele allele : Arrays.asList(base_A, base_C, base_T) ) {
                Double expLik;
                if ( idx % 2 == 0 )
                    expLik = allele == base_A ? -0.04 : -3.0;
                else
                    expLik = allele == base_C ? -0.04 : -3.0;
                Assert.assertEquals(likelihoods.get(allele), expLik);
            }
            idx++;
        }

        // and test downsampling for good measure

        final List<GATKRead> excessReads = new LinkedList<>();
        int prevSize = perReadAlleleLikelihoodMap.size();
        for ( int i = 0; i < 10 ; i++ ) {
            final GATKRead read = ArtificialReadUtils.createArtificialRead(header, "myExcessRead" + i, 0, 1, readLength);
            final byte[] bases = Utils.dupBytes((byte)'A', readLength);
            bases[0] = (byte)(i % 2 == 0 ? 'A' : 'C'); // every other base is a C

            // set the read's bases and quals
            read.setBases(bases);
            read.setBaseQualities(Utils.dupBytes((byte)30, readLength));
            for ( final Allele allele : Arrays.asList(base_A, base_C, base_T) ) {
                perReadAlleleLikelihoodMap.add(read,allele,allele==base_A ? -0.04 : -3.0);
            }
            Assert.assertEquals(perReadAlleleLikelihoodMap.size(), 1 + prevSize);
            prevSize = perReadAlleleLikelihoodMap.size();
        }

        Assert.assertEquals(perReadAlleleLikelihoodMap.size(), pileup.size() + 10);
        Assert.assertEquals(perReadAlleleLikelihoodMap.getAlleleStratifiedReadMap().get(base_A).size(), 60);
        perReadAlleleLikelihoodMap.performPerAlleleDownsampling(0.1);
        Assert.assertEquals(perReadAlleleLikelihoodMap.size(), (int) (0.9 * (pileup.size() + 10)));

        Map<Allele,List<GATKRead>> downsampledStrat = perReadAlleleLikelihoodMap.getAlleleStratifiedReadMap();
        Assert.assertEquals(downsampledStrat.get(base_A).size(), (pileup.size() / 2) - 1);
        Assert.assertEquals(downsampledStrat.get(base_C).size(), (pileup.size() / 2));
        Assert.assertEquals(downsampledStrat.get(base_T).size(), 0);
    }

    @DataProvider(name = "PoorlyModelledReadData")
    public Object[][] makePoorlyModelledReadData() {
        List<Object[]> tests = new ArrayList<>();

        // this functionality can be adapted to provide input data for whatever you might want in your data
        tests.add(new Object[]{10, 0.1, false, Arrays.asList(0.0)});
        tests.add(new Object[]{10, 0.1, true, Arrays.asList(-10.0)});
        tests.add(new Object[]{10, 0.1, false, Arrays.asList(0.0, -10.0)});
        tests.add(new Object[]{10, 0.1, true, Arrays.asList(-5.0, -10.0)});
        tests.add(new Object[]{100, 0.1, false, Arrays.asList(-5.0, -10.0)});
        tests.add(new Object[]{100, 0.01, true, Arrays.asList(-5.0, -10.0)});
        tests.add(new Object[]{100, 0.01, false, Arrays.asList(-5.0, -10.0, -3.0)});
        tests.add(new Object[]{100, 0.01, false, Arrays.asList(-5.0, -10.0, -2.0)});
        tests.add(new Object[]{100, 0.01, true, Arrays.asList(-5.0, -10.0, -4.2)});
        tests.add(new Object[]{100, 0.001, true, Arrays.asList(-5.0, -10.0)});
        tests.add(new Object[]{100, 0.001, false, Arrays.asList(-5.0, -10.0, 0.0)});

        return tests.toArray(new Object[][]{});
    }

    @Test(dataProvider = "PoorlyModelledReadData")
    public void testPoorlyModelledRead(final int readLen, final double maxErrorRatePerBase, final boolean expected, final List<Double> log10likelihoods) {
        final byte[] bases = Utils.dupBytes((byte)'A', readLen);
        final byte[] quals = Utils.dupBytes((byte) 40, readLen);

        final GATKRead read = ArtificialReadUtils.createArtificialRead(bases, quals, readLen + "M");

        final PerReadAlleleLikelihoodMap map = new PerReadAlleleLikelihoodMap();
        final boolean actual = PerReadAlleleLikelihoodMap.readIsPoorlyModelled(read, log10likelihoods, maxErrorRatePerBase);
        Assert.assertEquals(actual, expected);
    }


    @DataProvider(name = "RemovingPoorlyModelledReadData")
    public Object[][] makeRemovingPoorlyModelledReadData() {
        List<Object[]> tests = new ArrayList<>();

        // this functionality can be adapted to provide input data for whatever you might want in your data
        final int readLen = 10;
        for ( int nReads = 0; nReads < 4; nReads++ ) {
            for ( int nBad = 0; nBad <= nReads; nBad++ ) {
                final int nGood = nReads - nBad;
                tests.add(new Object[]{readLen, nReads, nBad, nGood});
            }
        }

        return tests.toArray(new Object[][]{});
    }

    @Test(dataProvider = "RemovingPoorlyModelledReadData")
    public void testRemovingPoorlyModelledReads(final int readLen, final int nReads, final int nBad, final int nGood) {
        final PerReadAlleleLikelihoodMap map = new PerReadAlleleLikelihoodMap();
        final Set<GATKRead> goodReads = new HashSet<>();
        final Set<GATKRead> badReads = new HashSet<>();
        for ( int readI = 0; readI < nReads; readI++ ) {
            final boolean bad = readI < nBad;
            final double likelihood = bad ? -100.0 : 0.0;

            final byte[] bases = Utils.dupBytes((byte)'A', readLen);
            final byte[] quals = Utils.dupBytes((byte) 40, readLen);

            final Allele allele = Allele.create(Strings.repeat('A', readI + 1));

            final GATKRead read = ArtificialReadUtils.createArtificialRead(bases, quals, readLen + "M");
            read.setName("readName" + readI);
            map.add(read, allele, likelihood);
            (bad ? badReads : goodReads).add(read);
        }

        final List<GATKRead> removedReads = map.filterPoorlyModelledReads(0.01);
        Assert.assertEquals(removedReads.size(), nBad, "nBad " + nBad + " nGood " + nGood);
        Assert.assertEquals(new HashSet<>(removedReads), badReads, "nBad " + nBad + " nGood " + nGood);
        Assert.assertEquals(map.size(), nGood, "nBad " + nBad + " nGood " + nGood);
        Assert.assertTrue(map.getReads().containsAll(goodReads), "nBad " + nBad + " nGood " + nGood);
        Assert.assertEquals(map.getReads().size(), nGood, "nBad " + nBad + " nGood " + nGood);
    }

    @DataProvider(name = "MostLikelyAlleleData")
    public Object[][] makeMostLikelyAlleleData() {
        List<Object[]> tests = new ArrayList<>();

        final Allele a = Allele.create("A");
        final Allele c = Allele.create("C");
        final Allele g = Allele.create("G");

        tests.add(new Object[]{Arrays.asList(a), Arrays.asList(Arrays.asList(0.0)), a, a});
        tests.add(new Object[]{Arrays.asList(a, c), Arrays.asList(Arrays.asList(0.0, -1.0)), a, a});
        tests.add(new Object[]{Arrays.asList(a, c), Arrays.asList(Arrays.asList(-1.0, 0.0)), c, c});
        tests.add(new Object[]{Arrays.asList(a, c, g), Arrays.asList(Arrays.asList(0.0, 0.0, -10.0)), a, a});
        tests.add(new Object[]{Arrays.asList(a, c, g), Arrays.asList(Arrays.asList(0.0, 0.0, -10.0)), a, a});
        tests.add(new Object[]{Arrays.asList(a, c, g),
                Arrays.asList(
                        Arrays.asList(0.0, -10.0, -10.0),
                        Arrays.asList(-100.0, 0.0, -10.0)),
                c, a});
        tests.add(new Object[]{Arrays.asList(a, c, g),
                Arrays.asList(
                        Arrays.asList(0.0, -10.0, -10.0),
                        Arrays.asList(-20.0, 0.0, -100.0)),
                c, a});

        return tests.toArray(new Object[][]{});
    }

    @Test(dataProvider = "MostLikelyAlleleData")
    public void testMostLikelyAllele(final List<Allele> alleles, final List<List<Double>> perReadlikelihoods, final Allele best, final Allele second) {
        final PerReadAlleleLikelihoodMap map = new PerReadAlleleLikelihoodMap();

        for ( int readI = 0; readI < perReadlikelihoods.size(); readI++ ) {
            final List<Double> likelihoods = perReadlikelihoods.get(readI);

            final byte[] bases = Utils.dupBytes((byte)'A', 10);
            final byte[] quals = Utils.dupBytes((byte) 30, 10);
            final GATKRead read = ArtificialReadUtils.createArtificialRead(bases, quals, "10M");
            read.setName("readName" + readI);

            for ( int i = 0; i < alleles.size(); i++ ) {
                final Allele allele = alleles.get(i);
                final double likelihood = likelihoods.get(i);
                map.add(read, allele, likelihood);
            }
        }

        final MostLikelyAllele mla = map.getMostLikelyDiploidAlleles();
        Assert.assertEquals(mla.getMostLikelyAllele(), best);
        Assert.assertEquals(mla.getSecondMostLikelyAllele(), second);
    }
}