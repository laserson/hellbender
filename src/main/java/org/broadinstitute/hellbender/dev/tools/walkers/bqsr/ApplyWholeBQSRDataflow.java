package org.broadinstitute.hellbender.dev.tools.walkers.bqsr;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.ValidationStringency;
import htsjdk.tribble.Feature;
import htsjdk.tribble.FeatureCodec;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.ArgumentCollection;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadProgramGroup;
import org.broadinstitute.hellbender.dev.pipelines.bqsr.ApplyWholeBQSRTransform;
import org.broadinstitute.hellbender.dev.pipelines.bqsr.BaseRecalibratorDataflowUtils;
import org.broadinstitute.hellbender.engine.FeatureDataSource;
import org.broadinstitute.hellbender.engine.FeatureInput;
import org.broadinstitute.hellbender.engine.FeatureManager;
import org.broadinstitute.hellbender.engine.dataflow.DataflowCommandLineProgram;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReadsDataflowSource;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.ApplyBQSRArgumentCollection;
import org.broadinstitute.hellbender.tools.ApplyBQSRWithoutMinQScoreArgumentCollection;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.dataflow.SmallBamWriter;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@CommandLineProgramProperties(
        summary = "Runs both BQSR phases on the input BAM.",
        oneLineSummary = "Runs both BQSR phases on the input BAM.",
        programGroup = ReadProgramGroup.class
)
public final class ApplyWholeBQSRDataflow extends DataflowCommandLineProgram {
    private static final long serialVersionUID = 1L;

    /**
     * Output path. Can be local or gs://
     */
    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, doc="Write recalibrated reads to this new BAM file")
    public String OUTPUT;

    /**
     * Command-line options for phase 1
     */
    @ArgumentCollection(doc = "all the command line arguments for BQSR and its covariates")
    public BaseRecalibrationArgumentCollection BRAC = new BaseRecalibrationArgumentCollection();

    /**
     * command-line options for phase 2
     */
    @ArgumentCollection
    public ApplyBQSRWithoutMinQScoreArgumentCollection bqsrArgs = new ApplyBQSRWithoutMinQScoreArgumentCollection();

    protected void setupPipeline(Pipeline pipeline) {
        if (BRAC.readArguments.getReadFilesNames().size()>1) {
            throw new UserException("Sorry, we only support a single input file for now.");
        }
        // workaround because the two collections have an argument in common and the parser doesn't want that
        ApplyBQSRArgumentCollection applyArgs = bqsrArgs.toApplyBQSRArgumentCollection(BRAC.PRESERVE_QSCORES_LESS_THAN);
        String filename = BRAC.readArguments.getReadFilesNames().get(0);
        ReadsDataflowSource readsSource = new ReadsDataflowSource(filename, pipeline);
        SAMFileHeader header = readsSource.getHeader();

        final List<SimpleInterval> intervals = BRAC.intervalArgumentCollection.getSpecifiedOrAllIntervals(header.getSequenceDictionary());

        PCollection<GATKRead> reads = readsSource.getReadPCollection(intervals, ValidationStringency.SILENT);
        PCollection<SimpleInterval> knownIntervals = ingestKnownIntervals(pipeline, BRAC.RAC.knownSites);

        PCollectionTuple inputs =
                PCollectionTuple.of(BaseRecalibratorDataflowUtils.readTag, reads)
                        .and(BaseRecalibratorDataflowUtils.intervalTag, knownIntervals);

        String referencePath = BRAC.referenceArguments.getReferenceFileName();

        // future way
        PCollection<GATKRead> output2 = inputs.apply(new ApplyWholeBQSRTransform(header, referencePath, BRAC, applyArgs));
        SmallBamWriter.writeToFile(pipeline, output2, header, OUTPUT);
    }


    /** list of known intervals -> PCollection */
    @SuppressWarnings("unchecked")
    private static PCollection<SimpleInterval> ingestKnownIntervals(final Pipeline pipeline, List<FeatureInput<Feature>> knownSites) {
        // known sites
        List<SimpleInterval> knownSitesLst = new ArrayList<>();
        for (FeatureInput<Feature> vcfSource : knownSites) {
            File featureFile = vcfSource.getFeatureFile();
            FeatureDataSource<Feature> source = new FeatureDataSource<Feature>(featureFile, (FeatureCodec<Feature, ?>) FeatureManager.getCodecForFile(featureFile), "KnownIntervals");
            for (Feature f : source) {
                knownSitesLst.add(new SimpleInterval(f));
            }
        }
        return pipeline.apply("known intervals ingest", Create.of(knownSitesLst).withCoder(SerializableCoder.of(SimpleInterval.class)));
    }
}
