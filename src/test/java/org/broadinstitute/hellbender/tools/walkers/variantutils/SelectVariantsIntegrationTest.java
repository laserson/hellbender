package org.broadinstitute.hellbender.tools.walkers.variantutils;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.IntegrationTestSpec;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class SelectVariantsIntegrationTest extends CommandLineProgramTest {

    // For now we're retaining hardcoded paths to the test files on the broad server
    // Any tests dependent on these files are in the testNG group "requiresRemoteAccess"
    // To enable these tests (which require access to the  broad files), set remoteTestsEnabled=true
    private static final boolean remoteTestsEnabled=false;

    public static final String b36KGReference = "/humgen/1kg/reference/human_b36_both.fasta";
    public static final String b37KGReference = "/humgen/1kg/reference/human_g1k_v37.fasta";
    public static final String GATKDataLocation = "/humgen/gsa-hpprojects/GATK/data/";
    public static final String comparisonDataLocation = GATKDataLocation + "Comparisons/";
    public static final String validationDataLocation = GATKDataLocation + "Validation_Data/";

    // these two files are used only by the 3 concordance/discordance tests
    public static final String b37hapmapGenotypes = comparisonDataLocation + "Validated/HapMap/3.3/genotypes_r27_nr.b37_fwd.vcf";
    public static final String hg19Reference = "/seq/references/Homo_sapiens_assembly19/v1/Homo_sapiens_assembly19.fasta";


    public static String baseTestString(String args, String testFile) {
        return " -R " + b36KGReference
                    + " --variant " + testFile
                    + " -o %s "
                    + " " + args;
    }

    @Test
    public void testSampleSelection() throws IOException {
        final String testFile = getToolTestDataDir() + "vcfexample2.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + hg19MiniReference
                        + " --variant " + testFile
                        + " -sn NA11918 "
                        + " -sr " // suppress reference file name in output for test differencing
                        + " -o %s ",
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_SimpleSelection.vcf")
        );

        spec.executeTest("testSampleSelection--" + testFile, this);
    }

    @Test
    public void testExpressionSelection() throws IOException {
        final String testFile = getToolTestDataDir() + "filteringDepthInFormat.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + hg19MiniReference
                        + " --variant " + testFile
                        + " -select 'DP < 7' "
                        + " -sr " // suppress reference file name in output for test differencing
                        + " -o %s ",
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_SimpleExpressionSelection.vcf")
        );

        spec.executeTest("testSimpleExpressionSelection--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testRepeatedLineSelection() throws IOException {
        final String testFile = getToolTestDataDir() + "test.dup.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" --interval 1 -sn A -sn B -sn C ", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_RepeatedLineSelection.vcf")
        );

        spec.executeTest("testRepeatedLineSelection--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testComplexSelection()  throws IOException {
        final String testFile = validationDataLocation + "test.filtered.maf_annotated.vcf";
        final String samplesFile = validationDataLocation + "SelectVariants.samples.txt";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" -sn A -se '[CDH]' -sf " + samplesFile + " --interval 1 -env -ef -select 'DP < 250'", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_ComplexSelection.vcf")
        );

        spec.executeTest("testComplexSelection--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testComplexSelectionWithNonExistingSamples()  throws IOException {
        final String testFile = validationDataLocation + "test.filtered.maf_annotated.vcf";
        final String samplesFile = validationDataLocation + "SelectVariants.samples.txt";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" --ALLOW_NONOVERLAPPING_COMMAND_LINE_SAMPLES -sn A -se '[CDH]' -sn Z -sn T -sf "
                        + samplesFile + " --interval 1 -env -ef -select 'DP < 250' ", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_ComplexSelectionWithNonExistingSamples.vcf")
        );
        spec.executeTest("testComplexSelectionWithNonExistingSamples--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testNonExistingFieldSelection()  throws IOException {
        final String testFile = validationDataLocation + "test.filtered.maf_annotated.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" --interval 1 -env -ef -select 'foo!=0||DP>0' ", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_NonExistingSelection.vcf")
        );

        spec.executeTest("testNonExistingSelection--" + testFile, this);
    }

    /**
     * Test excluding samples from file and sample name
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testSampleExclusionFromFileAndSeparateSample()  throws IOException {
        final String testFile = validationDataLocation + "test.filtered.maf_annotated.vcf";
        final String samplesFile = validationDataLocation + "SelectVariants.samples.txt";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" --interval 1:1-1000000 -xl_sn A -xl_sf " + samplesFile, testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_SampleExclusionFromFileAndSeparateSample.vcf")
        );

        spec.executeTest("testSampleExclusionFromFileAndSeparateSample--" + testFile, this);
    }

    /**
     * Test excluding samples from file
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testSampleExclusionJustFromFile()  throws IOException {
        final String testFile = validationDataLocation + "test.filtered.maf_annotated.vcf";
        final String samplesFile = validationDataLocation + "SelectVariants.samples.txt";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" --interval 1:1-1000000 -xl_sf " + samplesFile, testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_SampleExclusionJustFromFile.vcf")
        );

        spec.executeTest("testSampleExclusionJustFromFile--" + testFile, this);
    }

    /**
     * Test excluding samples from expression
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testSampleExclusionJustFromExpression()  throws IOException {
        final String testFile = validationDataLocation + "test.filtered.maf_annotated.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" --interval 1:1-1000000 -xl_se '[CDH]' ", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_SampleExclusionJustFromExpression.vcf")
        );

        spec.executeTest("testSampleExclusionJustFromExpression--" + testFile, this);
    }

    /**
     * Test excluding samples from negation expression
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testSampleExclusionJustFromNegationExpression()  throws IOException {
        final String testFile = validationDataLocation + "test.filtered.maf_annotated.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" --interval 1:1-1000000 -se '[^CDH]' ", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_SampleExclusionJustFromRegexExpression.vcf")
        );

        spec.executeTest("testSampleExclusionJustFromRegexExpression--" + testFile, this);
    }

    /**
     * Test including samples that are not in the VCF
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testSampleInclusionWithNonexistingSamples()  throws IOException {
        final String testFile = validationDataLocation + "test.filtered.maf_annotated.vcf";
        final String samplesFile = validationDataLocation + "SelectVariants.samples.txt";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" --interval 1:1-1000000 -sn A -sn Z -sn Q -sf " + samplesFile, testFile),
                1,
                UserException.BadInput.class
        );

        spec.executeTest("testSampleInclusionWithNonexistingSamples--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=false)
    public void testDiscordanceNoSampleSpecified() throws IOException {
        final String testFile = getToolTestDataDir() + "NA12878.hg19.example1.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + hg19Reference
                        + " --variant " + b37hapmapGenotypes
                        + " -o %s "
                        + " --lenient --interval 20:1012700-1020000 "
                        + " -disc " + testFile,
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_DiscordanceNoSampleSpecified.vcf")
        );

        spec.executeTest("testDiscordanceNoSampleSpecified--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=false)
    public void testDiscordance() throws IOException {
        final String testFile = getToolTestDataDir() + "NA12878.hg19.example1.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + hg19Reference
                        + " --variant " + b37hapmapGenotypes
                        + " -o %s "
                        + " --lenient -sn NA12878 --interval 20:1012700-1020000 "
                        + " -disc " + testFile,
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_Discordance.vcf")
        );

        spec.executeTest("testDiscordance--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=false)
    public void testConcordance()  throws IOException {
        final String testFile = getToolTestDataDir() + "NA12878.hg19.example1.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + hg19Reference
                        + " --variant " + testFile
                        + " -o %s "
                        + " -sn NA12878 --interval 20:1012700-1020000 "
                        + " -conc " + b37hapmapGenotypes
                        + " --lenient ",
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_Concordance.vcf")
        );

        spec.executeTest("testConcordance--" + testFile, this);
    }

    /**
     * Test including variant types.
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testVariantTypeSelection() throws IOException {
        final String testFile = getToolTestDataDir() + "complexExample1.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" -restrictAllelesTo MULTIALLELIC -selectType MIXED ",testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_VariantTypeSelection.vcf")
        );

        spec.executeTest("testVariantTypeSelection--" + testFile, this);
    }

    /**
     * Test excluding indels that are larger than the specified size
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testMaxIndelLengthSelection() throws IOException {
        final String testFile = getToolTestDataDir() + "complexExample1.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" -selectType INDEL --maxIndelSize 2 ", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_MaxIndelLengthSelection.vcf")
        );

        spec.executeTest("testMaxIndelLengthSelection--" + testFile, this);
    }

    /**
     * Test excluding indels that are smaller than the specified size
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testMinIndelLengthSelection() throws IOException {
        final String testFile = getToolTestDataDir() + "complexExample1.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
               baseTestString(" -selectType INDEL --minIndelSize 2 ", testFile),
               Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_MinIndelLengthSelection.vcf")
        );

        spec.executeTest("testMinIndelLengthSelection--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testRemoveMLE() throws IOException {
        final String testFile = getToolTestDataDir() + "vcfexample.withMLE.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" -sn NA12892 ", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_RemoveMLE.vcf")
        );

        spec.executeTest("testRemoveMLE--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testKeepOriginalAC() throws IOException {
        final String testFile = getToolTestDataDir() + "vcfexample.loseAlleleInSelection.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" --keepOriginalAC -sn NA12892 ", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_KeepOriginalAC.vcf")
        );

        spec.executeTest("testKeepOriginalAC--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testKeepOriginalACAndENV() throws IOException {
        final String testFile = getToolTestDataDir() + "vcfexample.loseAlleleInSelection.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" --keepOriginalAC -sn NA12892 -env -trimAlternates ", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_KeepOriginalACAndENV.vcf")
        );

        spec.executeTest("testKeepOriginalACAndENV--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testKeepOriginalDP() throws IOException {
        final String testFile = getToolTestDataDir() + "CEUtrioTest.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                        "-R " + b37KGReference
                            + " --variant " + testFile
                            + " -o %s "
                            + " --keepOriginalDP -sn NA12892 ",
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_KeepOriginalDP.vcf")
        );

        spec.executeTest("testKeepOriginalDP--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testMultipleRecordsAtOnePosition() throws IOException {
        final String testFile = getToolTestDataDir() + "selectVariants.onePosition.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" -select 'KG_FREQ < 0.5' ", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_MultipleRecordsAtOnePosition.vcf")
        );

        spec.executeTest("testMultipleRecordsAtOnePosition--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testNoGTs() throws IOException {
        final String testFile = getToolTestDataDir() + "vcf4.1.example.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec (
                " -R " + b37KGReference
                    + " --variant " + testFile
                    + " -o %s ",
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_NoGTs.vcf")
        );

        spec.executeTest("testNoGTs--" + testFile, this);
    }

    // @TODO: This test passed using the GATK test files before sequence dictionary validation was enabled in HB
    // but now fails with: UserException$LexicographicallySortedSequenceDictionary: A USER ERROR has occurred:
    // Lexicographically sorted human genome sequence detected in variants. From debugging it appears that the
    // variants file the source of the error message, but it still fails the same way even after running the
    // picard SortVcf tool on the .vcf using the b37KGReference reference dictionary.
    /*
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testSelectFromMultiAllelic() throws IOException {
        final String testFile = getToolTestDataDir() + "multi-allelic.bi-allelicInGIH.vcf";
        final String samplesFile = getToolTestDataDir() + "GIH.samples.list";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + b37KGReference
                    + " --variant " + testFile
                    + " -o %s "
                    + " -sf " + samplesFile + " --excludeNonVariants -trimAlternates",
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_MultiAllelicExcludeNonVar.vcf")
        );
        spec.executeTest("test select from multi allelic with excludeNonVariants --" + testFile, this);
    }
    */

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testMultiAllelicAnnotationOrdering() throws IOException {
        final String testFile = getToolTestDataDir() + "multi-allelic-ordering.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + b37KGReference
                    + " --variant " + testFile
                    + " -o %s "
                    + " -sn SAMPLE-CC -sn SAMPLE-CT -sn SAMPLE-CA --excludeNonVariants",
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_MultiAllelicAnnotationOrdering.vcf")
        );
        spec.executeTest("test multi allelic annotation ordering --" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testFileWithoutInfoLineInHeader() throws IOException {
        testFileWithoutInfoLineInHeader("testSelectVariants_FileWithoutInfoLineInHeader", IllegalStateException.class);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testFileWithoutInfoLineInHeaderWithOverride() throws IOException {
        testFileWithoutInfoLineInHeader("testSelectVariants_FileWithoutInfoLineInHeaderWithOverride", null);
    }

    private void testFileWithoutInfoLineInHeader(final String name, final Class<? extends Exception> expectedException) throws IOException {
        final String testFile = getToolTestDataDir() + "missingHeaderLine.vcf";
        final String outFile = getToolTestDataDir() + "expected/" + name + ".vcf";
        //final String cmd = "-R " + b36KGReference + " -sn NA12892 --variant:dbsnp "

        final String cmd = baseTestString(" -sn NA12892 " + (expectedException == null ? " --lenient" : ""), testFile);

        IntegrationTestSpec spec =
                expectedException != null
                        ? new IntegrationTestSpec(cmd, 1, expectedException)
                        : new IntegrationTestSpec(cmd, Arrays.asList(outFile));

        spec.executeTest(name, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testInvalidJexl() throws IOException {

        // NOTE: JexlEngine singleton construction in VariantContextUtils sets silent to false.
        // However VariantFiltration.initialize() sets setSilent(true) on the shared instance.
        // Just in case this test runs after a VariantFiltration in the same VM, always set silent back to false.
        htsjdk.variant.variantcontext.VariantContextUtils.engine.get().setSilent(false);

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + b37KGReference
                    + " -V " + getToolTestDataDir() + "ac0.vcf"
                    + " -o %s "
                    + " -select 'vc.getGenotype(\"FAKE_SAMPLE\").isHomRef()' ",
                1,
                UserException.class);
        spec.executeTest("InvalidJexl", this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testAlleleTrimming() throws IOException {
        final String testFile = getToolTestDataDir() + "forHardLeftAlignVariantsTest.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + b37KGReference
                        + " -V " + testFile
                        + " -o %s "
                        + " -sn NA12878 -env -trimAlternates ",
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_AlleleTrimming.vcf"));
        spec.executeTest("testAlleleTrimming", this);
    }

    @DataProvider(name="unusedAlleleTrimmingProvider")
    public Object[][] unusedAlleleTrimmingProvider() {
        final String expectedPath = getToolTestDataDir() + "expected/";
        return new Object[][] {
                {
                        getToolTestDataDir() + "forHardLeftAlignVariantsTest.vcf",
                        "-trimAlternates",
                        expectedPath + "testSelectVariants_UnusedAlleleHardLeftTrim.vcf"
                },
                {
                        getToolTestDataDir() + "forHardLeftAlignVariantsTest.vcf",
                        null,
                        expectedPath + "testSelectVariants_UnusedAlleleHardLeft.vcf"
                },
                {
                        getToolTestDataDir() + "multi-allelic-ordering.vcf",
                        "-sn SAMPLE-CC -sn SAMPLE-CT",
                        expectedPath + "testSelectVariants_UnusedAlleleCCCT.vcf"
                },
                {
                        getToolTestDataDir() + "multi-allelic-ordering.vcf",
                        "-sn SAMPLE-CC -sn SAMPLE-CT -env",
                        expectedPath + "testSelectVariants_UnusedAlleleCCCTEnv.vcf"
                },
                {
                        getToolTestDataDir() + "multi-allelic-ordering.vcf",
                        "-sn SAMPLE-CC -sn SAMPLE-CT -trimAlternates",
                        expectedPath + "testSelectVariants_UnusedAlleleCCCTTrim.vcf"
                },
                {
                        getToolTestDataDir() + "multi-allelic-ordering.vcf",
                        "-sn SAMPLE-CC -sn SAMPLE-CT -env -trimAlternates",
                        expectedPath + "testSelectVariants_UnusedAlleleCCCTTrimAltEnv.vcf"
                }
        };
    }

    @Test(dataProvider="unusedAlleleTrimmingProvider", groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testUnusedAlleleTrimming(final String vcf, final String extraArgs, final String expectedOutput) throws IOException {
        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + b37KGReference
                    + " -V " + vcf
                    + " -o %s "
                    + (extraArgs == null ? "" : extraArgs),
                Arrays.asList(expectedOutput)
        );

        spec.executeTest(
                String.format("testUnusedAlleleTrimming: (%s,%s)", new File(vcf).getName(), extraArgs == null ? "(none)" : extraArgs),
                this);
    }

    /**
     *  Test with an empty VCF file
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testEmptyVcfException() throws IOException {
        final String testFile = getToolTestDataDir() + "reallyEmpty.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString("", testFile),
                1,
                UserException.CouldNotReadInputFile.class
        );

        spec.executeTest("testEmptyVcfException--" + testFile, this);
    }

    /**
     * Test with a VCF file that is not a file
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testNotFileVcfException() throws IOException {
        final String testFile = getToolTestDataDir();

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString("", testFile),
                1,
                UserException.CouldNotReadInputFile.class
        );

        spec.executeTest("testNotFileVcfException--" + testFile, this);
    }

    /**
     * Test with a VCF file that does not exist
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testMissingVcfException() throws IOException {
        final String testFile = "test.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString("", testFile),
                1,
                UserException.CouldNotReadInputFile.class
        );

        spec.executeTest("testMissingVcfException--" + testFile, this);
    }

    /**
     * Test inverting the variant selection criteria by the -invertSelect argument
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testInvertSelection()  throws IOException {
        final String testFile = validationDataLocation + "test.filtered.maf_annotated.vcf";
        final String samplesFile = validationDataLocation + "SelectVariants.samples.txt";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" -sn A -se '[CDH]' -sf " + samplesFile + " --interval 1 -env -ef -select 'DP < 20000' -invertSelect ", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_InvertSelection.vcf")
        );

        spec.executeTest("testInvertSelection--" + testFile, this);
    }

    /**
     * Test inverting the variant selection criteria by inverting the JEXL expression logic following -select
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testInvertJexlSelection()  throws IOException {
        final String testFile = validationDataLocation + "test.filtered.maf_annotated.vcf";
        final String samplesFile = validationDataLocation + "SelectVariants.samples.txt";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" -sn A -se '[CDH]' -sf " + samplesFile + " -env -ef --interval 1 -select 'DP >= 20000' ", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_InvertJexlSelection.vcf")
        );

        spec.executeTest("testInvertJexlSelection--" + testFile, this);
    }

    /**
     * Test selecting variants with IDs
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testKeepSelectionID() throws IOException {
        final String testFile = getToolTestDataDir() + "complexExample1.vcf";
        final String idFile = getToolTestDataDir() + "complexExample1.vcf.id";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" -IDs " + idFile + " --interval 1 ", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_KeepSelectionID.vcf")
        );

        spec.executeTest("testKeepSelectionID--" + testFile, this);
    }

    /**
     * Test excluding variants with IDs
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testExcludeSelectionID() throws IOException {
        final String testFile = getToolTestDataDir() + "complexExample1.vcf";
        final String idFile = getToolTestDataDir() + "complexExample1.vcf.id";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" --interval 1 " + " -xlIDs " + idFile, testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_ExcludeSelectionID.vcf")
        );

        spec.executeTest("testExcludeSelectionID--" + testFile, this);
    }

    /**
     * Test excluding variant types
     */
    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testExcludeSelectionType() throws IOException {
        final String testFile = getToolTestDataDir() + "complexExample1.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                baseTestString(" -xlSelectType SNP ", testFile),
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_ExcludeSelectionType.vcf")
        );

        spec.executeTest("testExcludeSelectionType--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testMendelianViolationSelection() throws IOException {
        final String testFile = getToolTestDataDir() + "CEUtrioTest.vcf";
        final String pedFile = getToolTestDataDir() + "CEUtrio.ped";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + b37KGReference
                        + " --variant " + testFile
                        + " -o %s "
                        + " -ped " + pedFile
                        + " -mv -mvq 0 ",
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_MendelianViolationSelection.vcf")
        );

        spec.executeTest("testMendelianViolationSelection--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testInvertMendelianViolationSelection() throws IOException {
        final String testFile = getToolTestDataDir() + "CEUtrioTest.vcf";
        final String pedFile = getToolTestDataDir() + "CEUtrio.ped";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + b37KGReference
                        + " --variant " + testFile
                        + " -o %s "
                        + " -mv -mvq 0 -invMv "
                        + " -ped " + pedFile,
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_InvertMendelianViolationSelection.vcf")
        );

        spec.executeTest("testInvertMendelianViolationSelection--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testMaxFilteredGenotypesSelection() throws IOException {
        final String testFile = getToolTestDataDir() + "filteredSamples.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + b37KGReference
                        + " --variant " + testFile
                        + " -o %s"
                        + " --maxFilteredGenotypes 1 ",
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_MaxFilteredGenotypesSelection.vcf")
        );

        spec.executeTest("testMaxFilteredGenotypesSelection--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testMinFilteredGenotypesSelection() throws IOException {
        final String testFile = getToolTestDataDir() + "filteredSamples.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + b37KGReference
                        + " --variant " + testFile
                        + " -o %s "
                        + " --minFilteredGenotypes 2 ",
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_MinFilteredGenotypesSelection.vcf")
        );

        spec.executeTest("testMinFilteredGenotypesSelection--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testMaxFractionFilteredGenotypesSelection() throws IOException {
        final String testFile = getToolTestDataDir() + "filteredSamples.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + b37KGReference
                        + " --variant " + testFile
                        + " -o %s"
                        + " --maxFractionFilteredGenotypes 0.4 ",
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_MaxFractionFilteredGenotypesSelection.vcf")
        );

        spec.executeTest("testMaxFractionFilteredGenotypesSelection--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testMinFractionFilteredGenotypesSelection() throws IOException {
        final String testFile = getToolTestDataDir() + "filteredSamples.vcf";

        final  IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + b37KGReference
                        + " --variant " + testFile
                        + " -o %s"
                        + " --minFractionFilteredGenotypes 0.6 ",
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_MinFractionFilteredGenotypesSelection.vcf")
        );

        spec.executeTest("testMinFractionFilteredGenotypesSelection--" + testFile, this);
    }

    @Test(groups={"requiresRemoteAccess"}, enabled=remoteTestsEnabled)
    public void testSetFilteredGtoNocall() throws IOException {
        final String testFile = getToolTestDataDir() + "filteredSamples.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -R " + b37KGReference
                        + " --variant " + testFile
                        + " -o %s"
                        + " --setFilteredGtToNocall ",
                Arrays.asList(getToolTestDataDir() + "expected/" + "testSelectVariants_SetFilteredGtoNocall.vcf")
        );

        spec.executeTest("testSetFilteredGtoNocall--" + testFile, this);
    }
}
