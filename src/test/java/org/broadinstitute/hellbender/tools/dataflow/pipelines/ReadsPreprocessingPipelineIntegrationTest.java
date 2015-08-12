package org.broadinstitute.hellbender.tools.dataflow.pipelines;


import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.tools.IntegrationTestSpec;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReadsPreprocessingPipelineIntegrationTest extends CommandLineProgramTest {

    @DataProvider(name = "EndToEndTestData")
    public Object[][] getEndToEndTestData() {
        final String testDir = publicTestDir + "org/broadinstitute/hellbender/engine/";

        return new Object[][] {
                // This test case checks that the input and output bams are equal (which should be the case with stub tool implementations)
                { testDir + "reads_data_source_test1.bam", "EOSt9JOVhp3jkwE", testDir + "feature_data_source_test.vcf", new File(testDir + "reads_data_source_test1.bam") }
        };
    }

    @Test(dataProvider = "EndToEndTestData", groups = {"cloud"})
    public void testPipelineEndToEnd( final String inputBam, final String reference, final String knownSites, final File expectedOutput ) throws IOException {
        final File output = createTempFile("testPipelineEndToEnd_output", ".bam");
        List<String> argv = new ArrayList<>();

        argv.addAll(Arrays.asList("-" + StandardArgumentDefinitions.INPUT_SHORT_NAME, inputBam,
                                  "-" + StandardArgumentDefinitions.REFERENCE_SHORT_NAME, reference,
                                  "-BQSRKnownVariants", knownSites,
                                  "-" + StandardArgumentDefinitions.OUTPUT_SHORT_NAME, output.getAbsolutePath()));
        argv.addAll(getStandardDataflowArgumentsFromEnvironment());

        // Note: could use IntegrationTestSpec if we expand it a bit
        runCommandLine(argv);

        IntegrationTestSpec.assertEqualBamFiles(output, expectedOutput);
    }
}
// -I /Users/davidada/dev/git_projects/broad/hellbender_branch/src/test/resources/org/broadinstitute/hellbender/engine/reads_data_source_test1.bam
// -R EOSt9JOVhp3jkwE
// -BQSRKnownVariants /Users/davidada/dev/git_projects/broad/hellbender_branch/src/test/resources/org/broadinstitute/hellbender/engine/feature_data_source_test.vcf
// --apiKey AIzaSyDjKyy2KWcEVgtlOlJBdqy4O4oUVMxluE4
// --project broad-dsde-dev
// --staging gs://hellbender-test-davidada/staging/