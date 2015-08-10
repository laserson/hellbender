package org.broadinstitute.hellbender.engine.spark.datasources;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

public class ReadsSparkSourceUnitTest extends BaseTest {
    @DataProvider(name = "loadReads")
    public Object[][] loadReads() {
        return new Object[][]{
                {"src/test/resources/org/broadinstitute/hellbender/tools/BQSR/HiSeq.1mb.1RG.2k_lines.alternate.bam"},
        };
    }

    @Test(dataProvider = "loadReads")
    public void pairReadsAndVariantsTest(String bam) {
        SparkConf sparkConf = new SparkConf().setAppName("LoadReads")
                .setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "org.broadinstitute.hellbender.engine.spark.GATKRegistrator");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        ReadsSparkSource readSource = new ReadsSparkSource(ctx);
        JavaRDD<GATKRead> rddSerialReads = readSource.getSerialReads(bam);
        JavaRDD<GATKRead> rddParallelReads = readSource.getParallelReads(bam);

        List<GATKRead> serialReads = rddSerialReads.collect();
        List<GATKRead> parallelReads = rddParallelReads.collect();
        Assert.assertEquals(serialReads.size(), parallelReads.size());

        ctx.stop();
    }

}