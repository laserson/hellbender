package org.broadinstitute.hellbender.tools.dataflow.transforms.metrics.insertsize;

import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.common.collect.Sets;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.metrics.Header;
import htsjdk.samtools.util.Histogram;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.ArgumentCollectionDefinition;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.metrics.MetricAccumulationLevel;
import org.broadinstitute.hellbender.tools.dataflow.transforms.metrics.HistogramCombinerDataflow;
import org.broadinstitute.hellbender.tools.dataflow.transforms.metrics.HistogramDataflow;
import org.broadinstitute.hellbender.tools.dataflow.transforms.metrics.MetricsFileDataflow;
import org.broadinstitute.hellbender.tools.picard.analysis.InsertSizeMetrics;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class InsertSizeMetricsDataflowTransform extends PTransform<PCollection<GATKRead>, PCollection<MetricsFileDataflow<InsertSizeMetrics,Integer>>> {
    public static final long serialVersionUID = 1l;

    private final Arguments args;
    private final PCollectionView<List<Header>> metricHeaders;
    private final PCollectionView<SAMFileHeader> samHeader;

    /**
     * This selects the second in pair in order to avoid double counting inserts from pairs.
     * Ignore any non pair, duplicate or 0 length fragment because they don't make sense for count insert size.
     */
    private static final ReadFilter isSecondInMappedPair = r -> r.isPaired() &&
            !r.isUnmapped() &&
            !r.mateIsUnmapped() &&
            !r.isFirstOfPair() &&
            !(r.isSupplementaryAlignment() || r.isSecondaryAlignment()) &&
            !r.isDuplicate() &&
            r.getFragmentLength() != 0;

    public static class Arguments implements ArgumentCollectionDefinition, Serializable {
        public final static long serialVersionUID = 1l;

        @Argument(doc = "Generate mean, sd and plots by trimming the data down to MEDIAN + DEVIATIONS*MEDIAN_ABSOLUTE_DEVIATION. " +
                "This is done because insert size data typically includes enough anomalous values from chimeras and other " +
                "artifacts to make the mean and sd grossly misleading regarding the real distribution.")
        public double DEVIATIONS = 10;

        @Argument(shortName = "W", doc = "Explicitly sets the Histogram width, overriding automatic truncation of Histogram tail. " +
                "Also, when calculating mean and standard deviation, only bins <= HISTOGRAM_WIDTH will be included.", optional = true)
        public Integer HISTOGRAM_WIDTH = null;

        @Argument(shortName = "M", doc = "When generating the Histogram, discard any data categories (out of FR, TANDEM, RF) that have fewer than this " +
                "percentage of overall reads. (Range: 0 to 0.5).")
        public float MINIMUM_PCT = 0.05f;

        @Argument(shortName = "LEVEL", doc = "The level(s) at which to accumulate metrics.  ")
        public Set<MetricAccumulationLevel> METRIC_ACCUMULATION_LEVEL = EnumSet.of(MetricAccumulationLevel.ALL_READS);

        @Override
        public void validate() {
            if (MINIMUM_PCT < 0 || MINIMUM_PCT > 0.5) {
                throw new UserException.BadArgumentValue("MINIMUM_PCT", "It must be between 0 and 0.5 so all data categories don't get discarded.");
            }
        }
    }

    public InsertSizeMetricsDataflowTransform(Arguments args,PCollectionView<SAMFileHeader> samHeader, PCollectionView<List<Header>> metricHeaders) {
        args.validate();
        this.args = args;
        this.samHeader = samHeader;
        this.metricHeaders = metricHeaders;
    }

    @Override
    public PCollection<MetricsFileDataflow<InsertSizeMetrics, Integer>> apply(PCollection<GATKRead> input) {

        input.getPipeline().getCoderRegistry().registerCoder(InsertSizeMetrics.class, SerializableCoder.of(InsertSizeMetrics.class));

        PCollection<GATKRead> filtered = input.apply(Filter.by(isSecondInMappedPair)).setName("Filter singletons and first of pair");

        PCollection<KV<InsertSizeAggregationLevel, Integer>> kvPairs = filtered.apply(ParDo.withSideInputs(samHeader).of(new DoFn<GATKRead, KV<InsertSizeAggregationLevel, Integer>>() {
            private final static long serialVersionUID = 1l;

            @Override
            public void processElement(ProcessContext c) throws Exception {
                final GATKRead read = c.element();
                final SAMFileHeader samFileHeader = c.sideInput(samHeader);
                Integer metric = computeMetric(read);
                List<InsertSizeAggregationLevel> aggregationLevels = InsertSizeAggregationLevel.getKeysForAllAggregationLevels(read, samFileHeader, args.METRIC_ACCUMULATION_LEVEL);

                aggregationLevels.stream().forEach(k -> c.output(KV.of(k, metric)));
            }
        })).setName("Calculate metric and key")
                .setCoder(KvCoder.of(GenericJsonCoder.of(InsertSizeAggregationLevel.class), BigEndianIntegerCoder.of()));

        CombineFn<Integer, HistogramDataflow<Integer>, HistogramDataflow<Integer>> combiner = new HistogramCombinerDataflow<>();
        PCollection<KV<InsertSizeAggregationLevel,HistogramDataflow<Integer>>> histograms = kvPairs.apply(Combine.<InsertSizeAggregationLevel, Integer,HistogramDataflow<Integer>>perKey(combiner)).setName("Add reads to histograms");

        PCollection<KV<InsertSizeAggregationLevel, KV<InsertSizeAggregationLevel,HistogramDataflow<Integer>>>> reKeyedHistograms = histograms.apply(ParDo.of(new DoFn<KV<InsertSizeAggregationLevel, HistogramDataflow<Integer>>, KV<InsertSizeAggregationLevel, KV<InsertSizeAggregationLevel, HistogramDataflow<Integer>>>>() {
            public final static long serialVersionUID = 1l;

            @Override
            public void processElement(ProcessContext c) throws Exception {
                KV<InsertSizeAggregationLevel, HistogramDataflow<Integer>> histo = c.element();
                InsertSizeAggregationLevel oldKey = histo.getKey();
                InsertSizeAggregationLevel newKey = new InsertSizeAggregationLevel(null, oldKey.getLibrary(), oldKey.getReadGroup(), oldKey.getSample());
                c.output(KV.of(newKey, histo));
            }
        })).setName("Re-key histograms");

        PCollection <KV<InsertSizeAggregationLevel,MetricsFileDataflow< InsertSizeMetrics, Integer >>> metricsFiles = reKeyedHistograms.apply(Combine.perKey(new CombineInsertSizeHistogramsIntoMetricsFile(args.DEVIATIONS, args.HISTOGRAM_WIDTH, args.MINIMUM_PCT)))
                .setName("Add histograms and metrics to MetricsFile");

        PCollection<MetricsFileDataflow< InsertSizeMetrics, Integer >> metricsFilesNoKeys = metricsFiles.apply(ParDo.of(new DoFn<KV<?, MetricsFileDataflow<InsertSizeMetrics, Integer>>, MetricsFileDataflow<InsertSizeMetrics, Integer>>() {
            public final static long serialVersionUID = 1l;

            @Override
            public void processElement(ProcessContext c) throws Exception {
                c.output(c.element().getValue());
            }
        })).setName("Drop keys");

        PCollection<MetricsFileDataflow<InsertSizeMetrics,Integer>> singleMetricsFile = metricsFilesNoKeys.<PCollection<MetricsFileDataflow<InsertSizeMetrics, Integer>>>apply(Combine.globally(new CombineMetricsFiles()));

        PCollection<MetricsFileDataflow<InsertSizeMetrics,Integer>> singleMetricsFileWithHeaders = singleMetricsFile.apply(
                ParDo.named("add headers to MetricsFile")
                        .withSideInputs(this.metricHeaders)
                        .of(new AddHeadersToMetricsFile(metricHeaders)));

        return singleMetricsFileWithHeaders;
    }

    private Integer computeMetric(GATKRead read) {
        return Math.abs(read.getFragmentLength());
    }

    public static class  CombineMetricsFiles
    extends Combine.CombineFn<MetricsFileDataflow<InsertSizeMetrics,Integer>, MetricsFileDataflow<InsertSizeMetrics,Integer>, MetricsFileDataflow<InsertSizeMetrics,Integer>> {
        public static final long serialVersionUID = 1l;

        @Override
        public MetricsFileDataflow<InsertSizeMetrics,Integer> createAccumulator() {
            return new MetricsFileDataflow<>();
        }

        @Override
        public MetricsFileDataflow<InsertSizeMetrics,Integer> addInput(MetricsFileDataflow<InsertSizeMetrics,Integer> accumulator, MetricsFileDataflow<InsertSizeMetrics,Integer> input) {
            return combineMetricsFiles(accumulator, input);
        }

        private MetricsFileDataflow<InsertSizeMetrics,Integer> combineMetricsFiles(MetricsFileDataflow<InsertSizeMetrics,Integer> accumulator, MetricsFileDataflow<InsertSizeMetrics,Integer> input) {
            Set<Header> headers = Sets.newLinkedHashSet(accumulator.getHeaders());
            Set<Header> inputHeaders = Sets.newLinkedHashSet(input.getHeaders());
            inputHeaders.removeAll(headers);
            inputHeaders.forEach(accumulator::addHeader);

            accumulator.addAllMetrics(input.getMetrics());
            input.getAllHistograms().forEach(accumulator::addHistogram);
            return accumulator;
        }

        @Override
        public MetricsFileDataflow<InsertSizeMetrics,Integer> mergeAccumulators(Iterable<MetricsFileDataflow<InsertSizeMetrics,Integer>> accumulators) {
            MetricsFileDataflow<InsertSizeMetrics,Integer> base = createAccumulator();
            accumulators.forEach(accum -> combineMetricsFiles(base,  accum));
            return base;
        }

        @Override
        public MetricsFileDataflow<InsertSizeMetrics,Integer> extractOutput(MetricsFileDataflow<InsertSizeMetrics,Integer> accumulator) {
            List<InsertSizeMetrics> metrics = new ArrayList<>(accumulator.getMetrics());
            metrics.sort(InsertSizeMetrics.getInsertSizeMetricsComparator());
            MetricsFileDataflow<InsertSizeMetrics, Integer> sorted = new MetricsFileDataflow<>();
            sorted.addAllMetrics(metrics);
            accumulator.getAllHistograms().stream().sorted(Comparator.comparing(Histogram::getValueLabel)).forEach(sorted::addHistogram);
            accumulator.getHeaders().stream().forEach(sorted::addHeader);
            return sorted;
        }
    }

    private static class AddHeadersToMetricsFile extends DoFn<MetricsFileDataflow<InsertSizeMetrics, Integer>, MetricsFileDataflow<InsertSizeMetrics, Integer>> {
        private final static long serialVersionUID = 1l;
        private PCollectionView<List<Header>> metricHeaders;

        private AddHeadersToMetricsFile(PCollectionView<List<Header>> metricHeaders) {
            this.metricHeaders = metricHeaders;
        }

        @Override
        public void processElement(ProcessContext c) throws Exception {
            final List<Header> headers = c.sideInput(metricHeaders);
            final MetricsFileDataflow<InsertSizeMetrics, Integer> metricFile = c.element();
            headers.stream().forEach(metricFile::addHeader);
            c.output(metricFile);
        }
    }
}
