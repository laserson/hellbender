package org.broadinstitute.hellbender.tools.dataflow.pipelines;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.ArgumentCollection;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadProgramGroup;
import org.broadinstitute.hellbender.utils.logging.BunnyLog;
import org.broadinstitute.hellbender.engine.dataflow.DoFnWLog;
import org.broadinstitute.hellbender.engine.filters.CountingReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilterLibrary;
import org.broadinstitute.hellbender.engine.filters.WellformedReadFilter;
import org.broadinstitute.hellbender.tools.dataflow.transforms.bqsr.BaseRecalOutput;
import org.broadinstitute.hellbender.tools.dataflow.transforms.bqsr.BaseRecalibratorFn;
import org.broadinstitute.hellbender.tools.dataflow.transforms.bqsr.DataflowReadFilter;
import org.broadinstitute.hellbender.tools.dataflow.transforms.bqsr.BaseRecalibrationArgumentCollection;
import org.broadinstitute.hellbender.engine.dataflow.DataflowCommandLineProgram;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReadContextData;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReadsDataflowSource;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceDataflowSource;
import org.broadinstitute.hellbender.engine.dataflow.datasources.VariantsDataflowSource;
import org.broadinstitute.hellbender.tools.dataflow.transforms.bqsr.BaseRecalibratorTransform;
import org.broadinstitute.hellbender.engine.dataflow.transforms.composite.AddContextDataToRead;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.recalibration.RecalibrationTables;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SequenceDictionaryUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.baq.BAQ;
import org.broadinstitute.hellbender.utils.dataflow.BucketUtils;
import org.broadinstitute.hellbender.utils.dataflow.DataflowUtils;
import org.broadinstitute.hellbender.utils.io.IOUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToGATKReadAdapter;
import org.broadinstitute.hellbender.utils.read.SAMRecordToGATKReadAdapter;

import java.io.File;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;


/**
 * First pass of the base quality score recalibration -- Generates recalibration table based on various covariates
 * (such as read group, reported quality score, machine cycle, and nucleotide context).
 * <p>
 * Note: unmapped reads are ignored by this version of the tool.
 * <p>
 * This version uses the AddContextDataToRead transform.
 */
@CommandLineProgramProperties(
        summary = "First pass of the Base Quality Score Recalibration (BQSR) -- Generates recalibration table based on various user-specified covariates (such as read group, reported quality score, machine cycle, and nucleotide context).",
        oneLineSummary = "Generates recalibration table",
        programGroup = ReadProgramGroup.class
)
public class BaseRecalibratorDataflow extends DataflowCommandLineProgram implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
      * Reference window function for BQSR. For each read, returns an interval representing the span of
      * reference bases required by the BQSR algorithm for that read. Should be passed into the
      * {@link ReferenceDataflowSource} object for the {@link AddContextDataToRead} transform.
      */
    public static final SerializableFunction<GATKRead, SimpleInterval> BQSR_REFERENCE_WINDOW_FUNCTION =
            read -> BAQ.getReferenceWindowForRead(read, BAQ.DEFAULT_BANDWIDTH, BAQ.DEFAULT_INCLUDE_CLIPPED_BASES);

    public static final String NO_DBSNP_EXCEPTION = "This calculation is critically dependent on being able to skip over known variant sites. Please provide a VCF file containing known sites of genetic variation.";

    private final static Logger logger = LogManager.getLogger(BaseRecalibratorDataflow.class);
    // temporary file with the serialized recalibrationTables.
    private final static String TEMP_RECALTABLES = "temp-ds-recaltables";

    // ------------------------------------------
    // Command-line options

    /**
     * all the command line arguments for BQSR and its covariates
     */
    @ArgumentCollection(doc = "all the command line arguments for BQSR and its covariates")
    private transient final BaseRecalibrationArgumentCollection BRAC = new BaseRecalibrationArgumentCollection();

    // the ID of the reference set the user is asking for (extracted from BRAC.referenceArguments)
    protected String referenceID;

    @Argument(doc = "the known variants", shortName = "BQSRKnownVariants", fullName = "baseRecalibrationKnownVariants", optional = false)
    protected List<String> baseRecalibrationKnownVariants;

    /**
     * Path to save the serialized tables to. Local or GCS.
     */
    @Argument(doc = "Path to save the serialized recalibrationTables to. If running on the cloud, either leave outputTablesPath unset or point it to a GCS location.",
            shortName = "otp", fullName = "outputTablesPath", optional = true)
    protected String outputTablesPath = null;

    // ------------------------------------------

    // Whether we want to save the textual version of the output.
    private boolean saveTextualTables;

    private SAMFileHeader readsHeader;

    private transient BunnyLog bunny = new BunnyLog(logger);

    @Override
    protected void setupPipeline(Pipeline pipeline) {
        try {
            bunny.start("BaseRecalibratorDataflow");

            if (null==baseRecalibrationKnownVariants || baseRecalibrationKnownVariants.size()==0) {
                throw new UserException.CommandLineException(NO_DBSNP_EXCEPTION);
            }

            // TODO: once we move to our own thing, make RECAL_TABLE_FILE optional
            saveTextualTables = (null != BRAC.RAC.RECAL_TABLE_FILE);

            String referenceURL = BRAC.referenceArguments.getReferenceFileName();

            if (BRAC.readArguments.getReadFilesNames().size()!=1) {
                throw new UserException("Sorry, we only support a single reads input for now.");
            }
            String bam = BRAC.readArguments.getReadFilesNames().get(0);

            // Load the input bam
            final ReadsDataflowSource readsDataflowSource = new ReadsDataflowSource(bam, pipeline);
            readsHeader = readsDataflowSource.getHeader();
            final SAMSequenceDictionary readsDictionary = readsHeader.getSequenceDictionary();
            final List<SimpleInterval> intervals = BRAC.intervalArgumentCollection.intervalsSpecified() ? BRAC.intervalArgumentCollection.getIntervals(readsHeader.getSequenceDictionary())
                : IntervalUtils.getAllIntervalsForReference(readsHeader.getSequenceDictionary());
            final PCollectionView<SAMFileHeader> headerSingleton = ReadsDataflowSource.getHeaderView(pipeline, readsHeader);
            final PCollection<GATKRead> reads = readsDataflowSource.getReadPCollection(intervals)
                .apply(ParDo.of(new DoFnWLog<GATKRead, GATKRead>("unsam") {
                    private static final long serialVersionUID = 1l;
                    @Override
                    public void processElement(ProcessContext c) throws Exception {
                        // SAMRecords use too much memory when serialized. We convert to Genomic Reads.
                        GATKRead r = c.element();
                        if (!(r instanceof SAMRecordToGATKReadAdapter)) {
                            c.output(r);
                            return;
                        }
                        SAMRecordToGATKReadAdapter adapter = (SAMRecordToGATKReadAdapter)r;
                        c.output(new GoogleGenomicsReadToGATKReadAdapter(adapter.convertToGoogleGenomicsRead()));
                    }
                }));
            final PCollection<GATKRead> filteredReads = reads.apply(new DataflowReadFilter(BaseRecalibratorDataflow.readFilter(readsHeader), readsHeader));

            bunny.stepEnd("set up bam input");

            // Load the Variants and the Reference
            final VariantsDataflowSource variantsDataflowSource = new VariantsDataflowSource(baseRecalibrationKnownVariants, pipeline);

            final ReferenceDataflowSource referenceDataflowSource = new ReferenceDataflowSource(pipeline.getOptions(), referenceURL, BQSR_REFERENCE_WINDOW_FUNCTION);

            final SAMSequenceDictionary refDictionary = referenceDataflowSource.getReferenceSequenceDictionary(readsDictionary);

            checkSequenceDictionaries(refDictionary, readsDictionary);
            PCollectionView<SAMSequenceDictionary> refDictionaryView = pipeline.apply(Create.of(refDictionary)).setName("refDictionary").apply(View.asSingleton());
            bunny.stepEnd("load ref sequence dictionary");

            // Set up the data pipeline-style
            final PCollection<KV<GATKRead, ReadContextData>> readsWithContext = AddContextDataToRead.add(filteredReads, referenceDataflowSource, variantsDataflowSource);

            // run the base recalibrator, grab just the output we want.
            final PCollection<RecalibrationTables> recalibrationTable = readsWithContext.apply(new BaseRecalibratorTransform(headerSingleton, refDictionaryView, BRAC))
                .apply(ParDo.of(new DoFnWLog<BaseRecalOutput, RecalibrationTables>("getTables") {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void processElement(ProcessContext c) {
                        final BaseRecalOutput br = c.element();
                        c.output(br.getRecalibrationTables());
                    }
                }));

            // If saving textual output then we need to make sure we can get to the output
            if (saveTextualTables) {
                if (null == outputTablesPath) {
                    // we need those, so let's pick a temporary location for them.
                    outputTablesPath = pickTemporaryRecaltablesPath(isRemote(), stagingLocation);
                }
                DataflowUtils.SaveDestination dest = DataflowUtils.serializeSingleObject(recalibrationTable, outputTablesPath);
                if (isRemote() && dest == DataflowUtils.SaveDestination.LOCAL_DISK) {
                    throw new UserException("If running on the cloud, either leave outputTablesPath unset or point it to a GCS location.");
                }
            }
            bunny.stepEnd("setup");
        } catch (UserException|GATKException rx) {
            throw rx;
        } catch (Exception x) {
            throw new GATKException("Unexpected: " + x.getMessage(), x);
        }
    }

    @Override
    protected void afterPipeline(Pipeline p) {
        bunny.stepEnd("dataflow");
        if (saveTextualTables) {
            logger.info("Saving recalibration report to "+BRAC.RAC.RECAL_TABLE_FILE.getName());
            //  Get the table back and output it in text form to RAC.RECAL_TABLE.
            // TODO: if running on the cloud and the output destination is on the cloud, then it's faster to have a worker do it directly, without the file roundtrip.
            try (ObjectInputStream oin = new ObjectInputStream(BucketUtils.openFile(outputTablesPath, p.getOptions()))) {
                Object o = oin.readObject();
                RecalibrationTables rt = (RecalibrationTables) o;
                BaseRecalibratorFn.SaveTextualReport(BRAC.RAC.RECAL_TABLE_FILE, readsHeader, rt,  BRAC);
                bunny.stepEnd("repatriate_report");
            } catch (Exception e) {
                throw new GATKException("Unexpected: unable to read results file. (bug?)", e);
            }
        } else {
            // because the user didn't ask us to.
            logger.info("Not saving recalibration report");
        }
        bunny.end();
    }

    protected static String pickTemporaryRecaltablesPath(boolean remote, String stagingLocation) {
        if (remote) {
            return GcsPath.fromUri(stagingLocation).resolve(TEMP_RECALTABLES + ".sj").toString();
        } else {
            File outputTables = IOUtils.createTempFile(TEMP_RECALTABLES, ".sj");
            return outputTables.getPath();
        }
    }

    private static CountingReadFilter readFilter( final SAMFileHeader header ) {
        return new CountingReadFilter("Wellformed", new WellformedReadFilter(header))
            .and(new CountingReadFilter("Mapping_Quality_Not_Zero", ReadFilterLibrary.MAPPING_QUALITY_NOT_ZERO))
            .and(new CountingReadFilter("Mapping_Quality_Available", ReadFilterLibrary.MAPPING_QUALITY_AVAILABLE))
            .and(new CountingReadFilter("Mapped", ReadFilterLibrary.MAPPED))
            .and(new CountingReadFilter("Primary_Alignment", ReadFilterLibrary.PRIMARY_ALIGNMENT))
            .and(new CountingReadFilter("Not_Duplicate", ReadFilterLibrary.NOT_DUPLICATE))
            .and(new CountingReadFilter("Passes_Vendor_Quality_Check", ReadFilterLibrary.PASSES_VENDOR_QUALITY_CHECK));
    }

    private void checkSequenceDictionaries(final SAMSequenceDictionary refDictionary, SAMSequenceDictionary readsDictionary) {
        Utils.nonNull(refDictionary);
        Utils.nonNull(readsDictionary);
        SequenceDictionaryUtils.validateDictionaries("reference", refDictionary, "reads", readsDictionary);
    }




}
