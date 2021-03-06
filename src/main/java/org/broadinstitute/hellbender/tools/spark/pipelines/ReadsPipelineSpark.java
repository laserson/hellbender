package org.broadinstitute.hellbender.tools.spark.pipelines;

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import htsjdk.samtools.SAMFileHeader;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.ArgumentCollection;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.argumentcollections.IntervalArgumentCollection;
import org.broadinstitute.hellbender.cmdline.argumentcollections.OptionalIntervalArgumentCollection;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReadContextData;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceDataflowSource;
import org.broadinstitute.hellbender.engine.spark.AddContextDataToReadSpark;
import org.broadinstitute.hellbender.engine.spark.SparkCommandLineProgram;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSink;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.engine.spark.datasources.VariantsSparkSource;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.dataflow.pipelines.BaseRecalibratorDataflow;
import org.broadinstitute.hellbender.tools.recalibration.RecalibrationTables;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;
import org.broadinstitute.hellbender.utils.variant.Variant;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;


@CommandLineProgramProperties(
        summary = "Takes aligned reads (likely from BWA) and runs MarkDuplicates and BQSR. The final result is analysis-ready reads.",
        oneLineSummary = "Takes aligned reads (likely from BWA) and runs MarkDuplicates and BQSR. The final result is analysis-ready reads.",
        usageExample = "Hellbender ReadsPipelineSpark -I single.bam -R referenceURL -BQSRKnownVariants variants.vcf -O file:///tmp/output.bam",
        programGroup = SparkProgramGroup.class
)

/**
 * ReadsPreprocessingPipeline is our standard pipeline that takes aligned reads (likely from BWA) and runs MarkDuplicates
 * and BQSR. The final result is analysis-ready reads.
 */
public class ReadsPipelineSpark extends SparkCommandLineProgram {
    private static final long serialVersionUID = 1L;

    @Argument(doc = "uri for the input bam, either a local file path or a gs:// bucket path",
            shortName = StandardArgumentDefinitions.INPUT_SHORT_NAME, fullName = StandardArgumentDefinitions.INPUT_LONG_NAME,
            optional = false)
    protected String bam;

    @Argument(doc = "the output bam", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, optional = false)
    protected String output;

    @Argument(doc = "the reference name", shortName = StandardArgumentDefinitions.REFERENCE_SHORT_NAME,
            fullName = StandardArgumentDefinitions.REFERENCE_LONG_NAME, optional = false)
    protected String referenceURL;

    @Argument(doc = "the known variants", shortName = "BQSRKnownVariants", fullName = "baseRecalibrationKnownVariants", optional = true)
    protected List<String> baseRecalibrationKnownVariants;

    @ArgumentCollection
    protected IntervalArgumentCollection intervalArgumentCollection = new OptionalIntervalArgumentCollection();

    @Override
    protected void runPipeline(final JavaSparkContext ctx) {
        ReadsSparkSource readSource = new ReadsSparkSource(ctx);
        SAMFileHeader readsHeader = ReadsSparkSource.getHeader(ctx, bam);
        final List<SimpleInterval> intervals = intervalArgumentCollection.intervalsSpecified() ? intervalArgumentCollection.getIntervals(readsHeader.getSequenceDictionary())
                : IntervalUtils.getAllIntervalsForReference(readsHeader.getSequenceDictionary());
        JavaRDD<GATKRead> initialReads = readSource.getParallelReads(bam, intervals);

        JavaRDD<GATKRead> markedReads = initialReads.map(new MarkDuplicatesStub());
        VariantsSparkSource variantsSparkSource = new VariantsSparkSource(ctx);
        JavaRDD<Variant> bqsrKnownVariants = variantsSparkSource.getParallelVariants(baseRecalibrationKnownVariants);

        GCSOptions options = PipelineOptionsFactory.as(GCSOptions.class);
        options.setApiKey(apiKey);

        final ReferenceDataflowSource referenceDataflowSource = new ReferenceDataflowSource(options, referenceURL, BaseRecalibratorDataflow.BQSR_REFERENCE_WINDOW_FUNCTION);

        // TODO: Look into broadcasting the reference to all of the workers. This would make AddContextDataToReadSpark
        // TODO: and ApplyBQSRStub simpler (#855).
        JavaPairRDD<GATKRead, ReadContextData> rddReadContext = AddContextDataToReadSpark.add(markedReads, referenceDataflowSource, bqsrKnownVariants);

        JavaRDD<RecalibrationTables> tables = rddReadContext.map(new BaseRecalibratorStub());
        RecalibrationTables nestedIntegerArrays = null;
        if (tables != null) {
            nestedIntegerArrays = tables.collect().get(0);
        }
        final Broadcast<RecalibrationTables> broadcast = ctx.broadcast(nestedIntegerArrays);

        // ApplyBQSRStub.
        JavaRDD<GATKRead> finalReads = markedReads.map(v1 -> {
            RecalibrationTables value = broadcast.getValue();
            return v1;
        });

        try {
            ReadsSparkSink.writeReads(ctx, output, finalReads, readsHeader, ReadsWriteFormat.SHARDED);
        } catch (IOException e) {
            throw new GATKException("unable to write bam: " + e);
        }
    }

    @Override
    protected String getProgramName() {
        return "ReadsPipelineSpark";
    }

    private static class MarkDuplicatesStub implements Function<GATKRead, GATKRead> {
        private final static long serialVersionUID = 1L;
        @Override
        public GATKRead call(GATKRead v1) throws Exception {
            return v1;
        }
    }

    private static class BaseRecalibratorStub implements Function<Tuple2<GATKRead,ReadContextData>, RecalibrationTables> {
        private final static long serialVersionUID = 1L;
        @Override
        public RecalibrationTables call(Tuple2<GATKRead, ReadContextData> v1) throws Exception {
            return null;
        }
    }

}
