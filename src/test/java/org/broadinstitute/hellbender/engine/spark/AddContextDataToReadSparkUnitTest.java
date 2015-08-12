package org.broadinstitute.hellbender.engine.spark;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import htsjdk.samtools.SAMRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.dataflow.ReadsPreprocessingPipelineTestData;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReadContextData;
import org.broadinstitute.hellbender.engine.dataflow.datasources.RefAPIMetadata;
import org.broadinstitute.hellbender.engine.dataflow.datasources.RefAPISource;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.broadinstitute.hellbender.utils.test.FakeReferenceSource;
import org.broadinstitute.hellbender.utils.variant.Variant;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class AddContextDataToReadSparkUnitTest extends BaseTest {
    @DataProvider(name = "bases")
    public Object[][] bases() {
        Object[][] data = new Object[2][];
        List<Class<?>> classes = Arrays.asList(Read.class, SAMRecord.class);
        for (int i = 0; i < classes.size(); ++i) {
            Class<?> c = classes.get(i);
            ReadsPreprocessingPipelineTestData testData = new ReadsPreprocessingPipelineTestData(c);

            List<GATKRead> reads = testData.getReads();
            List<SimpleInterval> intervals = testData.getAllIntervals();
            List<Variant> variantList = testData.getVariants();
            List<KV<GATKRead, ReadContextData>> kvReadContextData = testData.getKvReadContextData();
            data[i] = new Object[]{reads, variantList, kvReadContextData, intervals};
        }
        return data;
    }

    @Test(dataProvider = "bases")
    public void addContextDataTest(List<GATKRead> reads, List<Variant> variantList,
                                   List<KV<GATKRead, ReadContextData>> kvReadContextData,
                                   List<SimpleInterval> intervals) {
        SparkConf sparkConf = new SparkConf().setAppName("JoinReadsWithVariantsTest")
                .setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<GATKRead> rddReads = ctx.parallelize(reads);
        JavaRDD<Variant> rddVariants = ctx.parallelize(variantList);

        RefAPISource mockSource = mock(RefAPISource.class, withSettings().serializable());
        for (SimpleInterval i : intervals) {
            when(mockSource.getReferenceBases(any(RefAPIMetadata.class), eq(i))).thenReturn(FakeReferenceSource.bases(i));
        }
        String referenceName = "refName";
        String refId = "0xbjfjd23f";
        Map<String, String> referenceNameToIdTable = Maps.newHashMap();
        referenceNameToIdTable.put(referenceName, refId);
        RefAPIMetadata refAPIMetadata = new RefAPIMetadata(referenceName, referenceNameToIdTable);
        RefAPISource.setRefAPISource(mockSource);

        JavaPairRDD<GATKRead, ReadContextData> rddActual = AddContextDataToReadSpark.JoinContextData(rddReads, refAPIMetadata, rddVariants);
        Map<GATKRead, ReadContextData> actual = rddActual.collectAsMap();

        Assert.assertEquals(actual.size(), kvReadContextData.size());
        for (KV<GATKRead, ReadContextData> kv : kvReadContextData) {
            ReadContextData readContextData = actual.get(kv.getKey());
            Assert.assertNotNull(readContextData);
            Assert.assertTrue(CollectionUtils.isEqualCollection(Lists.newArrayList(readContextData.getOverlappingVariants()),
                    Lists.newArrayList(kv.getValue().getOverlappingVariants())));
            Assert.assertEquals(readContextData.getOverlappingReferenceBases(), kv.getValue().getOverlappingReferenceBases());
        }

        ctx.close();
    }

}