package org.broadinstitute.hellbender.engine.spark.datasources;


import htsjdk.samtools.SAMFileHeader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ReadsSparkSinkUnitTest extends BaseTest {
    @DataProvider(name = "loadReads")
    public Object[][] loadReads() {
        return new Object[][]{
                {"src/test/resources/org/broadinstitute/hellbender/tools/BQSR/HiSeq.1mb.1RG.2k_lines.alternate.bam",
                        "HiSeq.1mb.1RG.2k_lines.alternate2.bam"},
        };
    }

    @Test(dataProvider = "loadReads")
    public void pairReadsAndVariantsTest(String inputBam, String outputBam) {
        SparkConf sparkConf = new SparkConf().setAppName("LoadReads")
                .setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "org.broadinstitute.hellbender.engine.spark.GATKRegistrator");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        ReadsSparkSource readSource = new ReadsSparkSource(ctx);
        JavaRDD<GATKRead> rddParallelReads = readSource.getParallelReads(inputBam);
        SAMFileHeader header = readSource.getHeader(inputBam);

        ReadsSparkSink.writeParallelReads(outputBam, rddParallelReads, header);

        JavaRDD<GATKRead> rddParallelReads2 = readSource.getParallelReads(outputBam + "/part-r-00000");

        Assert.assertEquals(rddParallelReads.count(), rddParallelReads2.count());

        ctx.stop();
    }

}