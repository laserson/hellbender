package org.broadinstitute.hellbender.engine.spark;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import htsjdk.samtools.SAMRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.dataflow.ReadsPreprocessingPipelineTestData;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.engine.spark.datasources.VariantsSparkSource;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.broadinstitute.hellbender.utils.variant.Variant;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

public class JoinReadsWithVariantsUnitTest extends BaseTest {
    @DataProvider(name = "pairedReadsAndVariants")
    public Object[][] pairedReadsAndVariants(){
        Object[][] data = new Object[2][];
        List<Class<?>> classes = Arrays.asList(Read.class, SAMRecord.class);
        for (int i = 0; i < classes.size(); ++i) {
            Class<?> c = classes.get(i);
            ReadsPreprocessingPipelineTestData testData = new ReadsPreprocessingPipelineTestData(c);

            List<GATKRead> reads = testData.getReads();
            List<Variant> variantList = testData.getVariants();
            List<KV<GATKRead, Iterable<Variant>>> kvReadiVariant = testData.getKvReadiVariantFixed();//getKvReadiVariant();
            data[i] = new Object[]{reads, variantList, kvReadiVariant};
        }
        return data;
    }

    @Test(dataProvider = "pairedReadsAndVariants")
    public void pairReadsAndVariantsTest(List<GATKRead> reads, List<Variant> variantList, List<KV<GATKRead, Iterable<Variant>>> kvReadiVariant) {
        SparkConf sparkConf = new SparkConf().setAppName("JoinReadsWithVariantsTest")
                .setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<GATKRead> rddReads = ctx.parallelize(reads);
        JavaRDD<Variant> rddVariants = ctx.parallelize(variantList);
        JavaPairRDD<GATKRead, Iterable<Variant>> actual = JoinReadsWithVariants.Join(rddReads, rddVariants);
        Map<GATKRead, Iterable<Variant>> gatkReadIterableMap = actual.collectAsMap();

        Assert.assertEquals(gatkReadIterableMap.size(), kvReadiVariant.size());
        for (KV<GATKRead, Iterable<Variant>> kv: kvReadiVariant) {
            List<Variant> variants = Lists.newArrayList(gatkReadIterableMap.get(kv.getKey()));
            Assert.assertTrue(!variants.isEmpty());
            HashSet<Variant> hashVariants = new HashSet<>(variants);
            final Iterable<Variant> iVariants = kv.getValue();
            HashSet<Variant> expectedHashVariants = Sets.newHashSet(iVariants);
            Assert.assertEquals(hashVariants, expectedHashVariants);
        }
        ctx.close();
    }

    @Test
    public void readFromFileTest() {
        String bam = "src/test/resources/org/broadinstitute/hellbender/tools/BQSR/HiSeq.1mb.1RG.2k_lines.alternate.bam";
        String vcf = "src/test/resources/org/broadinstitute/hellbender/tools/BQSR/dbsnp_132.b37.excluding_sites_after_129.chr17_69k_70k.vcf";

        SparkConf sparkConf = new SparkConf().setAppName("LoadVariants")
                .setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "org.broadinstitute.hellbender.engine.spark.GATKRegistrator");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        ReadsSparkSource readSource = new ReadsSparkSource(ctx);
        JavaRDD<GATKRead> reads = readSource.getParallelReads(bam);
        VariantsSparkSource variantsSparkSource = new VariantsSparkSource(ctx);
        JavaRDD<Variant> variants = variantsSparkSource.getParallelVariants(vcf);

        JavaPairRDD<GATKRead, Iterable<Variant>> readsiVariants = JoinReadsWithVariants.Join(reads, variants);
        Map<GATKRead, Iterable<Variant>> map = readsiVariants.collectAsMap();
        ctx.stop();
    }

}