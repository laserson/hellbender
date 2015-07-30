package org.broadinstitute.hellbender.engine.filters;


import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContext.Type;
import htsjdk.variant.variantcontext.VariantContextBuilder;

import org.broadinstitute.hellbender.utils.GenomeLoc;
import org.broadinstitute.hellbender.utils.test.BaseTest;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.util.*;

/**
 * Tests for the various VariantContext variant filter predicates
 */
public class VariantFilterUnitTest extends BaseTest {

    final Allele SnpRef = Allele.create("A", true);
    final Allele Snp = Allele.create("T");
    final Allele MnpRef = Allele.create("AAAAAAAAA", true);
    final Allele Mnp = Allele.create("CCCCCCCCC");

    VariantContext snpVC;
    VariantContext mnpVC;

    public VariantFilterUnitTest() throws FileNotFoundException {
        initGenomeLocParser();
        snpVC = createArtificialVC(
                "id1",
                hg19GenomeLocParser.createGenomeLoc("1", 42, 42),
                Arrays.asList(SnpRef, Snp)
        );

        mnpVC = createArtificialVC(
                "id3",
                hg19GenomeLocParser.createGenomeLoc("2", 2, 10),
                Arrays.asList(MnpRef, Mnp)
        );
    }

    /**
     * Create an artificial VariantContext
     *
     */
    private static VariantContext createArtificialVC(
            String id,
            GenomeLoc loc,
            List<Allele> alleles)
    {
        VariantContextBuilder vb = new VariantContextBuilder();
        vb.id(id);
        if (alleles != null)
            vb.alleles(alleles);
        if (loc != null)
            vb.loc(loc.getContig(), loc.getStart(), loc.getStop());
        return vb.make();
    }


    @DataProvider(name="intervalVCs")
    public Object[][] intervalTestVCs() {
        final List<GenomeLoc> loc1 = new ArrayList<>();
        loc1.add(hg19GenomeLocParser.createGenomeLoc("1", 42, 42));

        final List<GenomeLoc> loc2 = new ArrayList<>();
        loc2.add(hg19GenomeLocParser.createGenomeLoc("2", 2, 10));

        final List<GenomeLoc> bothLocs = new ArrayList<>();
        bothLocs.add(hg19GenomeLocParser.createGenomeLoc("1", 42, 42));
        bothLocs.add(hg19GenomeLocParser.createGenomeLoc("2", 2, 10));

        final List<GenomeLoc> noLoc = new ArrayList<>();
        bothLocs.add(hg19GenomeLocParser.createGenomeLoc("1", 100, 200));

        return new Object[][]{
                { snpVC, loc1, true },
                { snpVC, loc2, false },
                { snpVC, bothLocs, true },
                { snpVC, noLoc, false },
                { mnpVC, loc1, false },
                { mnpVC, loc2, true },
                { mnpVC, bothLocs, true },
                { mnpVC, noLoc, false },
        };
    }

    @Test(dataProvider="intervalVCs")
    public void testIntervalVariantFilter(VariantContext vc, List<GenomeLoc> locs, boolean expected) {
        IntervalVariantFilter ivf = new IntervalVariantFilter(locs);
        Assert.assertTrue(ivf.test(vc) == expected);
    }

    @DataProvider(name="includeIDsVCs")
    public Object[][] includeIDsTestVCs() {

        return new Object[][]{
                { snpVC, new String[]{"id1"}, true },
                { snpVC, new String[]{"id1", "id2"}, true },
                { snpVC, new String[]{"noid"}, false },
        };
    }

    @Test(dataProvider="includeIDsVCs")
    public void testIncludeIDsVariantFilter(VariantContext vc, String[] incIDs, boolean expected) {
        Set<String> idSet = new HashSet<>();
        idSet.addAll(Arrays.asList(incIDs));
        IncludeIDsVariantFilter iivf = new IncludeIDsVariantFilter(idSet);
        Assert.assertTrue(iivf.test(vc) == expected);
    }

    @DataProvider(name="excludeIDsVCs")
    public Object[][] excludeIDsTestVCs() {

        return new Object[][]{
                { snpVC, new String[]{"id1"}, false },
                { snpVC, new String[]{"id1", "id2"}, false },
                { snpVC, new String[]{"noid"}, true },
        };
    }

    @Test(dataProvider="excludeIDsVCs")
    public void testExcludeIDsVariantFilter(VariantContext vc, String[] exIDs, boolean expected) {
        Set<String> idSet = new HashSet<>();
        idSet.addAll(Arrays.asList(exIDs));
        ExcludeIDsVariantFilter eivf = new ExcludeIDsVariantFilter(idSet);
        Assert.assertTrue(eivf.test(vc) == expected);
    }

    @DataProvider(name="typeVCs")
    public Object[][] typeTestVCs() {

        return new Object[][]{
                { snpVC, new Type[]{Type.SNP}, true },
                { snpVC, new Type[]{Type.SNP, Type.MNP}, true },
                { snpVC, new Type[]{Type.INDEL}, false },
                { snpVC, new Type[]{Type.INDEL, Type.MIXED}, false },
                { mnpVC, new Type[]{Type.SNP}, false },
                { mnpVC, new Type[]{Type.SNP, Type.MNP}, true },
                { mnpVC, new Type[]{Type.INDEL}, false },
                { mnpVC, new Type[]{Type.INDEL, Type.MIXED}, false },
        };
    }

    @Test(dataProvider="typeVCs")
    public void testVariantTypeVariantFilter(VariantContext vc, Type[] types, boolean expected) {
        Set<Type> typesSet = new HashSet<>();
        typesSet.addAll(Arrays.asList(types));
        VariantTypesVariantFilter vtvf = new VariantTypesVariantFilter(typesSet);
        Assert.assertTrue(vtvf.test(vc) == expected);
    }
}
