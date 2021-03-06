package org.broadinstitute.hellbender.engine.spark.datasources;


import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.spark.SparkContextFactory;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadCoordinateComparator;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;


public class ReadsSparkSinkUnitTest extends BaseTest {
    private static String testDataDir = publicTestDir + "org/broadinstitute/hellbender/";

    @DataProvider(name = "loadReadsBAM")
    public Object[][] loadReadsBAM() {
        return new Object[][]{
                {testDataDir + "engine/dataflow/ReadsSource/HiSeq.1mb.1RG.2k_lines.bam", "ReadsSparkSinkUnitTest1.bam"},
                {testDataDir + "tools/BQSR/expected.HiSeq.1mb.1RG.2k_lines.bqsr.DIQ.alternate.bam", "ReadsSparkSinkUnitTest2.bam"},
        };
    }

    @DataProvider(name = "loadReadsADAM")
    public Object[][] loadReadsADAM() {
        return new Object[][]{
                {testDataDir + "engine/dataflow/ReadsSource/HiSeq.1mb.1RG.2k_lines.bam", "ReadsSparkSinkUnitTest1_ADAM"},
                {testDataDir + "tools/BQSR/expected.HiSeq.1mb.1RG.2k_lines.bqsr.DIQ.alternate.bam", "ReadsSparkSinkUnitTest2_ADAM"},
        };
    }

    @Test(dataProvider = "loadReadsBAM", groups = "spark")
    public void readsSinkTest(String inputBam, String outputFile) throws IOException {
        new File(outputFile).deleteOnExit();
        JavaSparkContext ctx = SparkContextFactory.getTestSparkContext();

        ReadsSparkSource readSource = new ReadsSparkSource(ctx);
        JavaRDD<GATKRead> rddParallelReads = readSource.getParallelReads(inputBam);
        SAMFileHeader header = ReadsSparkSource.getHeader(ctx, inputBam);

        ReadsSparkSink.writeReads(ctx, outputFile, rddParallelReads, header, ReadsWriteFormat.SINGLE);

        JavaRDD<GATKRead> rddParallelReads2 = readSource.getParallelReads(outputFile);
        final List<GATKRead> writtenReads = rddParallelReads2.collect();

        final ReadCoordinateComparator comparator = new ReadCoordinateComparator(header);
        // Assert that the reads are sorted.
        final int size = writtenReads.size();
        for (int i = 0; i < size-1; ++i) {
            final GATKRead smaller = writtenReads.get(i);
            final GATKRead larger = writtenReads.get(i + 1);
            Assert.assertTrue(comparator.compare(smaller, larger) < 0);
        }
        Assert.assertEquals(rddParallelReads.count(), rddParallelReads2.count());
    }

    @Test(dataProvider = "loadReadsADAM", groups = "spark")
    public void readsSinkADAMTest(String inputBam, String outputFile) throws IOException {
        new File(outputFile).deleteOnExit();
        JavaSparkContext ctx = SparkContextFactory.getTestSparkContext();

        ReadsSparkSource readSource = new ReadsSparkSource(ctx);
        JavaRDD<GATKRead> rddParallelReads = readSource.getParallelReads(inputBam);
        SAMFileHeader header = ReadsSparkSource.getHeader(ctx, inputBam);

        ReadsSparkSink.writeReads(ctx, outputFile, rddParallelReads, header, ReadsWriteFormat.ADAM);

        JavaRDD<GATKRead> rddParallelReads2 = readSource.getADAMReads(outputFile, header);
        Assert.assertEquals(rddParallelReads.count(), rddParallelReads2.count());

        // Test the round trip
        List<GATKRead> samList = rddParallelReads.collect();
        List<GATKRead> adamList = rddParallelReads2.collect();
        Comparator<GATKRead> comparator = new ReadCoordinateComparator(header);
        samList.sort(comparator);
        adamList.sort(comparator);
        for (int i = 0; i < samList.size(); i++) {
            SAMRecord expected = samList.get(i).convertToSAMRecord(header);
            SAMRecord observed = adamList.get(i).convertToSAMRecord(header);
            // manually test equality of some fields, as there are issues with roundtrip BAM -> ADAM -> BAM
            // see https://github.com/bigdatagenomics/adam/issues/823
            Assert.assertEquals(expected.getReadName(), observed.getReadName());
            Assert.assertEquals(expected.getAlignmentStart(), observed.getAlignmentStart());
            Assert.assertEquals(expected.getAlignmentEnd(), observed.getAlignmentEnd());
            Assert.assertEquals(expected.getFlags(), observed.getFlags());
            Assert.assertEquals(expected.getMappingQuality(), observed.getMappingQuality());
            Assert.assertEquals(expected.getMateAlignmentStart(), observed.getMateAlignmentStart());
            Assert.assertEquals(expected.getCigar(), observed.getCigar());
        }
    }
}
