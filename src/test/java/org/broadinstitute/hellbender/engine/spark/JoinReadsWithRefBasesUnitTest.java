package org.broadinstitute.hellbender.engine.spark;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.Maps;
import htsjdk.samtools.SAMRecord;
import junit.framework.TestCase;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.dev.tools.walkers.bqsr.BaseRecalibratorDataflow;
import org.broadinstitute.hellbender.engine.dataflow.ReadsPreprocessingPipelineTestData;
import org.broadinstitute.hellbender.engine.dataflow.datasources.RefAPIMetadata;
import org.broadinstitute.hellbender.engine.dataflow.datasources.RefAPISource;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import org.broadinstitute.hellbender.utils.test.FakeReferenceSource;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class JoinReadsWithRefBasesUnitTest extends TestCase {
    @DataProvider(name = "bases")
    public Object[][] bases(){
        Object[][] data = new Object[2][];
        List<Class<?>> classes = Arrays.asList(Read.class, SAMRecord.class);
        for (int i = 0; i < classes.size(); ++i) {
            Class<?> c = classes.get(i);
            ReadsPreprocessingPipelineTestData testData = new ReadsPreprocessingPipelineTestData(c);

            List<GATKRead> reads = testData.getReads();
            List<SimpleInterval> intervals = testData.getAllIntervals();
            List<KV<GATKRead, ReferenceBases>> kvReadRefBases = testData.getKvReadsRefBases();
            data[i] = new Object[]{reads, kvReadRefBases, intervals};
        }
        return data;
    }

    @Test(dataProvider = "bases")
    public void refBasesTest(List<GATKRead> reads, List<KV<GATKRead, ReferenceBases>> kvReadRefBases,
                             List<SimpleInterval> intervals) {
        SparkConf sparkConf = new SparkConf().setAppName("JoinReadsWithVariantsTest")
                .setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<GATKRead> rddReads = ctx.parallelize(reads);

        RefAPISource mockSource = mock(RefAPISource.class, withSettings().serializable());
        for (SimpleInterval i : intervals) {
            when(mockSource.getReferenceBases(any(RefAPIMetadata.class), eq(i))).thenReturn(FakeReferenceSource.bases(i));
        }

        String referenceName = "refName";
        String crazyName = "0xbjfjd23f";
        Map<String, String> referenceNameToIdTable = Maps.newHashMap();
        referenceNameToIdTable.put(referenceName, crazyName);
        RefAPIMetadata refAPIMetadata = new RefAPIMetadata(referenceName, referenceNameToIdTable, BaseRecalibratorDataflow.BQSR_REFERENCE_WINDOW_FUNCTION, "fakeApiKey");
        RefAPISource.setRefAPISource(mockSource);

        JavaPairRDD<GATKRead, ReferenceBases> rddResult = JoinReadsWithRefBases.Pair(refAPIMetadata, rddReads);
        Map<GATKRead, ReferenceBases> result = rddResult.collectAsMap();

        for (KV<GATKRead, ReferenceBases> kv : kvReadRefBases) {
            ReferenceBases referenceBases = result.get(kv.getKey());
            Assert.assertNotNull(referenceBases);
            Assert.assertEquals(kv.getValue(),referenceBases);
        }
        ctx.close();
    }

}