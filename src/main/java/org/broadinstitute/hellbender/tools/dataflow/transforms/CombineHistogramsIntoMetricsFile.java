package org.broadinstitute.hellbender.tools.dataflow.transforms;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.values.KV;
import htsjdk.samtools.util.Histogram;
import org.broadinstitute.hellbender.tools.picard.analysis.InsertSizeMetrics;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CombineHistogramsIntoMetricsFile
        extends Combine.CombineFn<KV<InsertSizeAggregationLevel, DataflowHistogram<Integer>>, CombineHistogramsIntoMetricsFile, InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics, Integer>> {
    public final static long serialVersionUID = 1l;

    private final double DEVIATIONS;
    private final Integer HISTOGRAM_WIDTH;
    private final float MINIMUM_PERCENT;

    private final Map<InsertSizeAggregationLevel, DataflowHistogram<Integer>> histograms = new HashMap<>();

    public CombineHistogramsIntoMetricsFile(double deviations, Integer histogramWidth, float minimumPercent) {
        this.DEVIATIONS = deviations;
        this.HISTOGRAM_WIDTH = histogramWidth;
        this.MINIMUM_PERCENT = minimumPercent;
    }

    public void addHistogramToMetricsFile(InsertSizeAggregationLevel aggregationLevel, DataflowHistogram<Integer> histogram, InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics, Integer> metricsFile, double totalInserts) {
        final double total = histogram.getCount();

        // Only include a category if it has a sufficient percentage of the data in it
        if ((total > totalInserts * MINIMUM_PERCENT)) {
            final InsertSizeMetrics metrics = new InsertSizeMetrics();
            metrics.SAMPLE = aggregationLevel.getSample();
            metrics.LIBRARY = aggregationLevel.getLibrary();
            metrics.READ_GROUP = aggregationLevel.getReadGroup();
            metrics.PAIR_ORIENTATION = aggregationLevel.getOrientation();
            metrics.READ_PAIRS = (long) total;
            metrics.MAX_INSERT_SIZE = (int) histogram.getMax();
            metrics.MIN_INSERT_SIZE = (int) histogram.getMin();
            metrics.MEDIAN_INSERT_SIZE = histogram.getMedian();
            metrics.MEDIAN_ABSOLUTE_DEVIATION = histogram.getMedianAbsoluteDeviation();

            final double median = histogram.getMedian();
            double covered = 0;
            double low = median;
            double high = median;

            while (low >= histogram.getMin() || high <= histogram.getMax()) {
                @SuppressWarnings("unchecked")
                final htsjdk.samtools.util.Histogram<Integer>.Bin lowBin = histogram.get((int) low);

                if (lowBin != null) covered += lowBin.getValue();

                if (low != high) {
                    @SuppressWarnings("unchecked")
                    final Histogram<Integer>.Bin highBin = histogram.get((int) high);
                    if (highBin != null) covered += highBin.getValue();
                }

                final double percentCovered = covered / total;
                final int distance = (int) (high - low) + 1;
                if (percentCovered >= 0.1 && metrics.WIDTH_OF_10_PERCENT == 0)
                    metrics.WIDTH_OF_10_PERCENT = distance;
                if (percentCovered >= 0.2 && metrics.WIDTH_OF_20_PERCENT == 0)
                    metrics.WIDTH_OF_20_PERCENT = distance;
                if (percentCovered >= 0.3 && metrics.WIDTH_OF_30_PERCENT == 0)
                    metrics.WIDTH_OF_30_PERCENT = distance;
                if (percentCovered >= 0.4 && metrics.WIDTH_OF_40_PERCENT == 0)
                    metrics.WIDTH_OF_40_PERCENT = distance;
                if (percentCovered >= 0.5 && metrics.WIDTH_OF_50_PERCENT == 0)
                    metrics.WIDTH_OF_50_PERCENT = distance;
                if (percentCovered >= 0.6 && metrics.WIDTH_OF_60_PERCENT == 0)
                    metrics.WIDTH_OF_60_PERCENT = distance;
                if (percentCovered >= 0.7 && metrics.WIDTH_OF_70_PERCENT == 0)
                    metrics.WIDTH_OF_70_PERCENT = distance;
                if (percentCovered >= 0.8 && metrics.WIDTH_OF_80_PERCENT == 0)
                    metrics.WIDTH_OF_80_PERCENT = distance;
                if (percentCovered >= 0.9 && metrics.WIDTH_OF_90_PERCENT == 0)
                    metrics.WIDTH_OF_90_PERCENT = distance;
                if (percentCovered >= 0.99 && metrics.WIDTH_OF_99_PERCENT == 0)
                    metrics.WIDTH_OF_99_PERCENT = distance;

                --low;
                ++high;
            }

            // Trim the Histogram down to get rid of outliers that would make the chart useless.
            final htsjdk.samtools.util.Histogram<Integer> trimmedHisto = histogram; //alias it
            int actualWidth = inferHistogramWidth(HISTOGRAM_WIDTH, metrics, DEVIATIONS);

            trimmedHisto.trimByWidth(actualWidth);

            metrics.MEAN_INSERT_SIZE = trimmedHisto.getMean();
            metrics.STANDARD_DEVIATION = trimmedHisto.getStandardDeviation();

            trimmedHisto.setBinLabel("insert_size");
            trimmedHisto.setValueLabel(aggregationLevel.getValueLabel());
            metricsFile.addHistogram(trimmedHisto);
            metricsFile.addMetric(metrics);
        }
    }

    /**
     * If histogramWidth is null infer a value for it
     */
    private static int inferHistogramWidth(Integer histogramWidth, InsertSizeMetrics metrics, double deviations) {
        if (histogramWidth == null) {
            return (int) (metrics.MEDIAN_INSERT_SIZE + (deviations * metrics.MEDIAN_ABSOLUTE_DEVIATION));
        } else {
            return histogramWidth;
        }
    }

    @Override
    public CombineHistogramsIntoMetricsFile createAccumulator() {
        return new CombineHistogramsIntoMetricsFile(this.DEVIATIONS, this.HISTOGRAM_WIDTH, this.MINIMUM_PERCENT);
    }

    @Override
    public CombineHistogramsIntoMetricsFile addInput(CombineHistogramsIntoMetricsFile accumulator, KV<InsertSizeAggregationLevel, DataflowHistogram<Integer>> input) {
        accumulator.histograms.put(input.getKey(), input.getValue());
        return accumulator;
    }

    @Override
    public CombineHistogramsIntoMetricsFile mergeAccumulators(Iterable<CombineHistogramsIntoMetricsFile> accumulators) {
        Map<InsertSizeAggregationLevel, DataflowHistogram<Integer>> histograms = StreamSupport.stream(accumulators.spliterator(), false)
                .flatMap(a -> a.histograms.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        CombineHistogramsIntoMetricsFile accum = createAccumulator();
        accum.histograms.putAll(histograms);
        return accum;
    }

    @Override
    public InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics, Integer> extractOutput(CombineHistogramsIntoMetricsFile accumulator) {
        final InsertSizeMetricsDataflowTransform.MetricsFileDataflow<InsertSizeMetrics, Integer> metricsFile = new InsertSizeMetricsDataflowTransform.MetricsFileDataflow<>();


        double totalInserts = accumulator.histograms.values()
                .stream()
                .mapToDouble(Histogram::getCount)
                .sum();

        Map<InsertSizeAggregationLevel, DataflowHistogram<Integer>> sortedHistograms = new TreeMap<>(Comparator.comparing((InsertSizeAggregationLevel a) -> a.getSample() != null ? a.getSample() : "")
                .thenComparing(a -> a.getLibrary() != null ? a.getLibrary() : "")
                .thenComparing(a -> a.getReadGroup() != null ? a.getReadGroup() : ""));
        sortedHistograms.putAll(accumulator.histograms);
        sortedHistograms.entrySet().forEach(kv -> addHistogramToMetricsFile(kv.getKey(), kv.getValue(), metricsFile, totalInserts));


        return metricsFile;
    }

}
