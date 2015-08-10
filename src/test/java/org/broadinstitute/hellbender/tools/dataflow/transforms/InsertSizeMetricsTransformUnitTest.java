package org.broadinstitute.hellbender.tools.dataflow.transforms;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import htsjdk.samtools.metrics.Header;
import htsjdk.samtools.metrics.MetricsFile;
import htsjdk.samtools.metrics.StringHeader;
import htsjdk.samtools.util.Histogram;
import htsjdk.samtools.util.TestUtil;
import org.broadinstitute.hellbender.engine.dataflow.GATKTestPipeline;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReadsDataflowSource;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.metrics.MetricAccumulationLevel;
import org.broadinstitute.hellbender.tools.dataflow.transforms.metrics.HistogramCombinerDataflow;
import org.broadinstitute.hellbender.tools.dataflow.transforms.metrics.MetricsFileDataflow;
import org.broadinstitute.hellbender.tools.dataflow.transforms.metrics.HistogramDataflow;
import org.broadinstitute.hellbender.tools.dataflow.transforms.metrics.insertsize.InsertSizeMetricsDataflowTransform;
import org.broadinstitute.hellbender.tools.picard.analysis.CollectInsertSizeMetrics;
import org.broadinstitute.hellbender.tools.picard.analysis.InsertSizeMetrics;
import org.broadinstitute.hellbender.utils.dataflow.DataflowUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;



public final class InsertSizeMetricsTransformUnitTest{

    private static final String SMALL_BAM = "org/broadinstitute/hellbender/tools/picard/analysis/CollectInsertSizeMetrics/insert_size_metrics_test.bam";
    private static final String METRICS_PATH = "org/broadinstitute/hellbender/metrics/";

    @DataProvider(name = "testFiles")
    public Object[][] testFiles(){
        return new Object[][] {
                {SMALL_BAM, METRICS_PATH + "insertSizeMetricsResultsAllWays.metric", EnumSet.allOf(MetricAccumulationLevel.class)},
                {SMALL_BAM, METRICS_PATH + "insertSizeMetric_All", EnumSet.of(MetricAccumulationLevel.ALL_READS)},
                {SMALL_BAM, METRICS_PATH + "insertSizeMetric_Sample", EnumSet.of(MetricAccumulationLevel.SAMPLE)},
                {SMALL_BAM, METRICS_PATH + "insertSizeMetric_Read_Group", EnumSet.of(MetricAccumulationLevel.READ_GROUP)},
                {SMALL_BAM, METRICS_PATH + "insertSizeMetric_Library", EnumSet.of(MetricAccumulationLevel.LIBRARY)},
                {METRICS_PATH + "HiSeq.1mb.1RG.2k_lines.bam", METRICS_PATH + "largeMetricsRun.metrics", EnumSet.allOf(MetricAccumulationLevel.class)}
        };
    }

    @Test(groups = "dataflow", dataProvider = "testFiles")
    public void testInsertSizeMetricsTransformOnFile(String bamName, String expectedMetrics, Set<MetricAccumulationLevel> accumulationLevels) throws IOException {
        final File expectedMetricsFile = new File(BaseTest.publicTestDir, expectedMetrics);
        final File bam = new File(BaseTest.publicTestDir, bamName);
        final Pipeline p = GATKTestPipeline.create();
        DataflowUtils.registerGATKCoders(p);

        ReadsDataflowSource source = new ReadsDataflowSource(bam.getAbsolutePath(), p);
        PCollection<GATKRead> preads = source.getReadPCollection();

        final List<Header> defaultHeaders = Arrays.asList(new StringHeader("Time stamp"), new StringHeader("--someString"));
        final PCollectionView<List<Header>> metricHeaders = p.apply(Create.of(defaultHeaders).withCoder(SerializableCoder.of(Header.class))).apply(View.asList());


        final InsertSizeMetricsDataflowTransform.Arguments args = new InsertSizeMetricsDataflowTransform.Arguments();
        args.METRIC_ACCUMULATION_LEVEL = accumulationLevels;
        InsertSizeMetricsDataflowTransform transform = new InsertSizeMetricsDataflowTransform(args, source.getHeaderView(), metricHeaders);

        PCollection<MetricsFileDataflow<InsertSizeMetrics,Integer>> presult = preads.apply(transform);
        DirectPipelineRunner.EvaluationResults result = (DirectPipelineRunner.EvaluationResults)p.run();
        final MetricsFileDataflow<InsertSizeMetrics, Integer> resultMetrics = result.getPCollection(presult).get(0);
        final MetricsFile<InsertSizeMetrics, Integer> expected = new MetricsFileDataflow<>();
        try( BufferedReader in = new BufferedReader(new FileReader(expectedMetricsFile))){
            expected.read(in);
        }

        assertMetricsFilesEqualUpToOrder(resultMetrics, expected);
    }

    @Test(groups = "dataflow", dataProvider = "testFiles")
    public void testInsertSizeMetricsTransformAgainstCollectInsertSizeMetrics(String bamName, String ignoreMe, Set<MetricAccumulationLevel> accumulationLevels) throws IOException {
        final CollectInsertSizeMetrics collectInsertSizeMetrics = new CollectInsertSizeMetrics();
        final File bam = new File(BaseTest.publicTestDir, bamName);
        collectInsertSizeMetrics.METRIC_ACCUMULATION_LEVEL = accumulationLevels;
        collectInsertSizeMetrics.HISTOGRAM_FILE = BaseTest.createTempFile("histogram","pdf");
        collectInsertSizeMetrics.PRODUCE_PLOT = false;
        collectInsertSizeMetrics.INPUT = bam;
        final File expectedMetricsFile = BaseTest.createTempFile("expectedMetric", "insertSizeMetric");
        collectInsertSizeMetrics.OUTPUT = expectedMetricsFile;
        collectInsertSizeMetrics.runTool();

        final Pipeline p = GATKTestPipeline.create();
        DataflowUtils.registerGATKCoders(p);

        ReadsDataflowSource source = new ReadsDataflowSource(bam.getAbsolutePath(), p);
        PCollection<GATKRead> preads = source.getReadPCollection();

        final List<Header> defaultHeaders = Arrays.asList(new StringHeader("Time stamp"), new StringHeader("--someString"));
        final PCollectionView<List<Header>> metricHeaders = p.apply(Create.of(defaultHeaders).withCoder(SerializableCoder.of(Header.class))).apply(View.asList());


        final InsertSizeMetricsDataflowTransform.Arguments args = new InsertSizeMetricsDataflowTransform.Arguments();
        args.METRIC_ACCUMULATION_LEVEL = accumulationLevels;
        InsertSizeMetricsDataflowTransform transform = new InsertSizeMetricsDataflowTransform(args, source.getHeaderView(), metricHeaders);

        PCollection<MetricsFileDataflow<InsertSizeMetrics,Integer>> presult = preads.apply(transform);
        DirectPipelineRunner.EvaluationResults result = (DirectPipelineRunner.EvaluationResults)p.run();
        final MetricsFileDataflow<InsertSizeMetrics, Integer> resultMetrics = result.getPCollection(presult).get(0);
        final MetricsFile<InsertSizeMetrics, Integer> expected = new MetricsFileDataflow<>();
        try( BufferedReader in = new BufferedReader(new FileReader(expectedMetricsFile))){
            expected.read(in);
        }

        assertMetricsFilesEqualUpToOrder(resultMetrics, expected);

    }

    @Test
    public void testCollectInsertSizeMetric() throws IOException {
        final CollectInsertSizeMetrics collectInsertSizeMetrics = new CollectInsertSizeMetrics();
        final File bam = new File(BaseTest.publicTestDir, SMALL_BAM);
        collectInsertSizeMetrics.METRIC_ACCUMULATION_LEVEL = EnumSet.of(MetricAccumulationLevel.READ_GROUP);
        collectInsertSizeMetrics.HISTOGRAM_FILE = BaseTest.createTempFile("histogram","pdf");
        collectInsertSizeMetrics.PRODUCE_PLOT = false;
        collectInsertSizeMetrics.INPUT = bam;
        final File expectedMetricsFile = BaseTest.createTempFile("expectedMetric", "insertSizeMetric");
        collectInsertSizeMetrics.OUTPUT = expectedMetricsFile;
        collectInsertSizeMetrics.runTool();

    }

    @Test
    public void testBadCase() throws IOException {
        final CollectInsertSizeMetrics collectInsertSizeMetrics = new CollectInsertSizeMetrics();
        final File bam = new File(BaseTest.publicTestDir, SMALL_BAM);

        final Pipeline p = GATKTestPipeline.create();
        DataflowUtils.registerGATKCoders(p);

        ReadsDataflowSource source = new ReadsDataflowSource(bam.getAbsolutePath(), p);
        PCollection<GATKRead> preads = source.getReadPCollection();

        final List<Header> defaultHeaders = Arrays.asList(new StringHeader("Time stamp"), new StringHeader("--someString"));
        final PCollectionView<List<Header>> metricHeaders = p.apply(Create.of(defaultHeaders).withCoder(SerializableCoder.of(Header.class))).apply(View.asList());


        final InsertSizeMetricsDataflowTransform.Arguments args = new InsertSizeMetricsDataflowTransform.Arguments();
        args.METRIC_ACCUMULATION_LEVEL = EnumSet.of(MetricAccumulationLevel.READ_GROUP);
        InsertSizeMetricsDataflowTransform transform = new InsertSizeMetricsDataflowTransform(args, source.getHeaderView(), metricHeaders);

        PCollection<MetricsFileDataflow<InsertSizeMetrics,Integer>> presult = preads.apply(transform);
        DirectPipelineRunner.EvaluationResults result = (DirectPipelineRunner.EvaluationResults)p.run();
    }


    @Test
    public void testHistogrammer(){
        List<Integer> records = IntStream.rangeClosed(1,1000).boxed().collect(Collectors.toList());
        Combine.CombineFn<Integer, HistogramDataflow<Integer>, HistogramDataflow<Integer>> combiner = new HistogramCombinerDataflow<>();

        HistogramDataflow<Integer> result = combiner.apply(records);
        Assert.assertEquals(result.getCount(),1000.0);
        Assert.assertEquals(result.getMax(),1000.0);
        Assert.assertEquals(result.getMin(),1.0);
    }


    public <K,V> List<KV<K,V>> kvZip(Iterable<K> keys, Iterable<V> values){
        Iterator<K> keysIter = keys.iterator();
        Iterator<V> valuesIter = values.iterator();
        List<KV<K, V>> kvs = new ArrayList<>();
        while(keysIter.hasNext() && valuesIter.hasNext()){
            kvs.add(KV.of(keysIter.next(), valuesIter.next()));
        }
        if(keysIter.hasNext() || valuesIter.hasNext()){
            throw new GATKException("there were different numbers of keys and values");
        }
        return kvs;
    }

    private HistogramDataflow<Integer> createDummyHistogram() {
        HistogramDataflow<Integer> h1= new HistogramDataflow<>();
        h1.addInput(10);
        h1.addInput(20);
        h1.addInput(10);
        return h1;
    }

    @Test
    public void testCombineMetricsFilePTransform(){
        final Pipeline p = GATKTestPipeline.create();
        DataflowUtils.registerGATKCoders(p);

        MetricsFileDataflow<InsertSizeMetrics,Integer> mf1 = new MetricsFileDataflow<>();
        mf1.addMetric(new InsertSizeMetrics());
        mf1.addHeader(new StringHeader("header1"));
        mf1.addHistogram(createDummyHistogram());

        MetricsFileDataflow<InsertSizeMetrics,Integer> mf2 = new MetricsFileDataflow<>();
        mf2.addMetric(new InsertSizeMetrics());
        mf2.addHeader(new StringHeader("header2"));
        mf2.addHistogram(createDummyHistogram());

        MetricsFileDataflow<InsertSizeMetrics,Integer> mf3 = new MetricsFileDataflow<>();
        mf3.addMetric(new InsertSizeMetrics());
        mf3.addHeader(new StringHeader("header3"));


        PCollection<MetricsFileDataflow<InsertSizeMetrics, Integer>> files = p.apply(Create.of(ImmutableList.of(mf1, mf2, mf3)));
        PCollection<MetricsFileDataflow<InsertSizeMetrics, Integer>> combined = files.apply(Combine.globally(new InsertSizeMetricsDataflowTransform.CombineMetricsFiles()));
        DirectPipelineRunner.EvaluationResults results =  (DirectPipelineRunner.EvaluationResults)p.run();
        MetricsFileDataflow<InsertSizeMetrics, Integer> metricsFile = results.getPCollection(combined).get(0);

        Assert.assertEquals(Sets.newHashSet(metricsFile.getHeaders()), Sets.newHashSet(new StringHeader("header1"), new StringHeader("header2"), new StringHeader("header3")));
        Assert.assertEquals(metricsFile.getAllHistograms().size(), 2);
    }


    @Test
    public void testCombineMetricsFiles(){
        Combine.CombineFn<MetricsFileDataflow<InsertSizeMetrics,Integer>,
                MetricsFileDataflow<InsertSizeMetrics,Integer>,
                MetricsFileDataflow<InsertSizeMetrics,Integer>> combiner =
                new InsertSizeMetricsDataflowTransform.CombineMetricsFiles();

        MetricsFileDataflow<InsertSizeMetrics, Integer> mf1 = getInsertSizeMetricsIntegerMetricsFileDataflow();

        MetricsFileDataflow<InsertSizeMetrics,Integer> mf2 = new MetricsFileDataflow<>();
        mf2.addMetric(new InsertSizeMetrics());
        mf2.addHeader(new StringHeader("header2"));

        MetricsFileDataflow<InsertSizeMetrics,Integer> mf3 = new MetricsFileDataflow<>();
        mf3.addMetric(new InsertSizeMetrics());
        mf3.addHeader(new StringHeader("header3"));

        @SuppressWarnings("unchecked")
        MetricsFileDataflow<InsertSizeMetrics, Integer> combined = combiner.apply(ImmutableList.of(mf1, mf2, mf3));

        Assert.assertEquals(combined.getMetrics().size(), 3);
        Assert.assertEquals(combined.getAllHistograms().size(), 0);
        Assert.assertEquals(combined.getHeaders().size(), 3);

    }

    private static MetricsFileDataflow<InsertSizeMetrics, Integer> getInsertSizeMetricsIntegerMetricsFileDataflow() {
        MetricsFileDataflow<InsertSizeMetrics,Integer> mf1 = new MetricsFileDataflow<>();
        mf1.addMetric(new InsertSizeMetrics());
        mf1.addHeader(new StringHeader("header1"));
        return mf1;
    }

    public static class PrintLn<T> extends DoFn<T,String>{
        public static final long serialVersionUID = 1l;

        @Override
        public void processElement(ProcessContext c) throws Exception {
            String str = c.element().toString();
            System.out.println(str);
            c.output(str);
        }
    }

    @Test
    public void dataflowSerializeMetricsFileTest(){
        MetricsFileDataflow<InsertSizeMetrics,Integer> metrics = new MetricsFileDataflow<>();
        metrics.addHistogram(new HistogramDataflow<>());

        @SuppressWarnings("unchecked")
        MetricsFileDataflow<InsertSizeMetrics, Integer> newMetrics =
                SerializableUtils.ensureSerializableByCoder(SerializableCoder.of(MetricsFileDataflow.class), metrics, "error");
        Assert.assertEquals(newMetrics.getAllHistograms(),metrics.getAllHistograms());
    }

    @Test
    public void javaSerializeMetricsFileTest() throws IOException, ClassNotFoundException {
        final MetricsFileDataflow<InsertSizeMetrics,Integer> metrics = new MetricsFileDataflow<>();
        metrics.addHistogram(new HistogramDataflow<>());
        final MetricsFileDataflow<InsertSizeMetrics,Integer> deserializedMetrics = TestUtil.serializeAndDeserialize(metrics);

        Assert.assertEquals(deserializedMetrics.getAllHistograms(), metrics.getAllHistograms());
    }

    private void assertMetricsFilesEqualUpToOrder(MetricsFile<InsertSizeMetrics,Integer> result, MetricsFile<InsertSizeMetrics,Integer> expected){

            Assert.assertEquals(result.getNumHistograms(), expected.getNumHistograms(), "Unequal numbers of histograms " +
                    "\nResult"+ result.toString() + "\nExpected: " + expected.toString());
            final List<KV<Histogram<Integer>, Histogram<Integer>>> histograms = kvZip(getSortedHistograms(result),
                    getSortedHistograms(expected));
            histograms.stream().forEach(kv -> assertHistogramEqualIgnoreZeroes(kv.getKey(), kv.getValue()));

        Assert.assertEquals(result.getMetrics(), expected.getMetrics());

        //time stamps and command lines will differ just check the number.
//        Assert.assertEquals(result.getHeaders().size(), expected.getHeaders().size(), "Incorrect number of headers: " +
//                "expected " + expected.getHeaders().size() + " found " + result.getHeaders().size() +".\n" + "Result:\n" +
//                result.toString() + "\n Expected:\n" + expected.toString());

    }

    private List<Histogram<Integer>> getSortedHistograms(MetricsFile<InsertSizeMetrics, Integer> result) {
        return result.getAllHistograms().stream().sorted(Comparator.comparing(Histogram::getValueLabel)).collect(Collectors.toList());
    }


    @SuppressWarnings("rawtypes")
    private  <A extends Comparable,B extends Comparable> void assertHistogramEqualIgnoreZeroes(Histogram<A> a, Histogram<B> b) {
        final Set<Map.Entry<?,?>> aBins = a.entrySet().stream().filter(entry -> entry.getValue().getValue() > 0.0).collect(Collectors.toSet());
        final Set<Map.Entry<?,?>> bBins = b.entrySet().stream().filter(entry -> entry.getValue().getValue() > 0.0).collect(Collectors.toSet());
        Assert.assertEquals(aBins, bBins);
    }
}
