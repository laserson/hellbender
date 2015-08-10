package org.broadinstitute.hellbender.engine.spark.datasources;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.broadinstitute.hellbender.utils.variant.Variant;
import org.broadinstitute.hellbender.utils.variant.VariantContextVariantAdapter;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

import static org.broadinstitute.hellbender.utils.variant.VariantContextVariantAdapterTest.createVariantContextVariantAdapterForTesting;

public class VariantsSparkSourceUnitTest extends BaseTest {
    @DataProvider(name = "loadVariants")
    public Object[][] loadVariants() {
        return new Object[][]{
                {"src/test/resources/org/broadinstitute/hellbender/tools/BQSR/dbsnp_132.b37.excluding_sites_after_129.chr17_69k_70k.vcf"},
        };
    }

    @Test(dataProvider = "loadVariants")
    public void pairReadsAndVariantsTest(String vcf) {
        SparkConf sparkConf = new SparkConf().setAppName("loadVariants")
                .setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "org.broadinstitute.hellbender.engine.spark.GATKRegistrator");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        VariantsSparkSource variantsSparkSource = new VariantsSparkSource(ctx);
        JavaRDD<Variant> rddSerialVariants =
                variantsSparkSource.getSerialVariants(vcf).map(v1 -> createVariantContextVariantAdapterForTesting((VariantContextVariantAdapter) v1, new UUID(0L, 0L)));
        JavaRDD<Variant> rddParallelVariants =
                variantsSparkSource.getParallelVariants(vcf).map(v1 -> createVariantContextVariantAdapterForTesting((VariantContextVariantAdapter) v1, new UUID(0L, 0L)));

        List<Variant> serialVariants = rddSerialVariants.collect();
        List<Variant> parallelVariants = rddParallelVariants.collect();
        Assert.assertEquals(parallelVariants, serialVariants);

        ctx.stop();
    }

}