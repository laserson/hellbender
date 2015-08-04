package org.broadinstitute.hellbender.tools.dataflow.pipelines;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.metrics.Header;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.ArgumentCollection;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.argumentcollections.IntervalArgumentCollection;
import org.broadinstitute.hellbender.cmdline.argumentcollections.OptionalIntervalArgumentCollection;
import org.broadinstitute.hellbender.cmdline.programgroups.DataFlowProgramGroup;
import org.broadinstitute.hellbender.engine.dataflow.DataflowCommandLineProgram;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReadsDataflowSource;
import org.broadinstitute.hellbender.tools.dataflow.MetricsFileDataflow;
import org.broadinstitute.hellbender.tools.dataflow.transforms.InsertSizeMetricsDataflowTransform;
import org.broadinstitute.hellbender.tools.picard.analysis.InsertSizeMetrics;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.dataflow.DataflowUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.List;

@CommandLineProgramProperties(summary = "Collect insert size metrics on dataflow" , oneLineSummary = "insert size metrics", programGroup = DataFlowProgramGroup.class)
public class InsertSizeMetricsDataflow extends DataflowCommandLineProgram {
    public static final long serialVersionUID = 1l;

    @Argument(doc = "uri for the input bam, either a local file path or a gs:// bucket path",
            shortName = StandardArgumentDefinitions.INPUT_SHORT_NAME, fullName = StandardArgumentDefinitions.INPUT_LONG_NAME,
            optional = false)
    protected String bam;

    @Argument(doc="a prefix for the dataflow output files", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, optional = false)
    protected String outputFile;


    @ArgumentCollection
    protected IntervalArgumentCollection intervalArgumentCollection = new OptionalIntervalArgumentCollection();

    @ArgumentCollection
    InsertSizeMetricsDataflowTransform.Arguments arguments = new InsertSizeMetricsDataflowTransform.Arguments();


    @Override
    protected void setupPipeline( Pipeline pipeline ) {

        // Load the reads.
        final ReadsDataflowSource readsDataflowSource = new ReadsDataflowSource(bam, pipeline);
        final SAMFileHeader readsHeader = readsDataflowSource.getHeader();

        final List<SimpleInterval> intervals = intervalArgumentCollection.intervalsSpecified() ? intervalArgumentCollection.getIntervals(readsHeader.getSequenceDictionary())
                : IntervalUtils.getAllIntervalsForReference(readsHeader.getSequenceDictionary());

        final PCollectionView<SAMFileHeader> headerSingleton = ReadsDataflowSource.getHeaderView(pipeline, readsHeader);
        final PCollection<GATKRead> initialReads = readsDataflowSource.getReadPCollection(intervals);

        final List<Header> defaultHeaders = getDefaultHeaders();
        final PCollectionView<List<Header>> metricHeaders = pipeline.apply(Create.of(defaultHeaders).withCoder(SerializableCoder.of(Header.class))).apply(View.asList());

        final InsertSizeMetricsDataflowTransform insertSizeMetricsDataflowTransform = new InsertSizeMetricsDataflowTransform(arguments, headerSingleton, metricHeaders);

        final PCollection<MetricsFileDataflow<InsertSizeMetrics, Integer>> metricFile = initialReads.apply(insertSizeMetricsDataflowTransform);
        final PCollection<String> strings = metricFile.apply(DataflowUtils.convertToString());
        strings.apply(TextIO.Write.to(outputFile));
    }
}
