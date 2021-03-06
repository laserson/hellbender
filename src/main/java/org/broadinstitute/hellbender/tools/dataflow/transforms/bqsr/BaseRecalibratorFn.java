package org.broadinstitute.hellbender.tools.dataflow.transforms.bqsr;

import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.util.Locatable;
import org.broadinstitute.hellbender.engine.dataflow.DoFnWLog;
import org.broadinstitute.hellbender.engine.ReferenceDataSource;
import org.broadinstitute.hellbender.engine.ReferenceMemorySource;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReadContextData;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.recalibration.QuantizationInfo;
import org.broadinstitute.hellbender.tools.recalibration.ReadCovariates;
import org.broadinstitute.hellbender.tools.recalibration.RecalUtils;
import org.broadinstitute.hellbender.tools.recalibration.RecalibrationTables;
import org.broadinstitute.hellbender.tools.recalibration.covariates.StandardCovariateList;
import org.broadinstitute.hellbender.tools.walkers.bqsr.ReadRecalibrationInfo;
import org.broadinstitute.hellbender.tools.walkers.bqsr.RecalibrationEngine;
import org.broadinstitute.hellbender.transformers.ReadTransformer;
import org.broadinstitute.hellbender.utils.BaseUtils;
import org.broadinstitute.hellbender.utils.MathUtils;
import org.broadinstitute.hellbender.utils.baq.BAQ;
import org.broadinstitute.hellbender.utils.clipping.ReadClipper;
import org.broadinstitute.hellbender.utils.read.AlignmentUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import org.broadinstitute.hellbender.utils.recalibration.EventType;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import org.broadinstitute.hellbender.utils.variant.Variant;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

/**
 * DoFn for BaseRecalibrator.
 * Takes in reads + contextual data (overlapping reference bases and variants), spits out RecalibrationTables.
 */
public final class BaseRecalibratorFn extends DoFnWLog<KV<GATKRead, ReadContextData>, RecalibrationTables> {
    private static final long serialVersionUID = 1L;

    private PCollectionView<SAMFileHeader> headerView;
    private transient SAMFileHeader header;
    private PCollectionView<SAMSequenceDictionary> referenceSequenceDictionaryView;
    private transient SAMSequenceDictionary referenceSequenceDictionary;

    private BaseRecalibrationArgumentCollection BRAC;

    /**
     * list to hold the all the covariate objects that were requested (required + standard + experimental)
     */
    private StandardCovariateList covariates;

    private RecalibrationEngine recalibrationEngine;

    // true when we are about to process the first element of a bundle.
    private boolean firstInBundle = true;
    private int minimumQToUse;
    private long nReadsProcessed = 0;

    private BAQ baq; // BAQ the reads on the fly to generate the alignment uncertainty vector
    private final static byte NO_BAQ_UNCERTAINTY = (byte) '@';

    // -------------------------------------------------------------
    // public functions (and constructors, those go first)

    /**
     * DoFn for BaseRecalibrator.
     * Takes in reads + contextual data (overlapping reference bases and variants), spits out RecalibrationTables.
     */
    public BaseRecalibratorFn(PCollectionView<SAMFileHeader> headerView, PCollectionView<SAMSequenceDictionary> referenceSequenceDictionaryView, BaseRecalibrationArgumentCollection BRAC) {
        super("BaseRecalibratorFn");
        this.headerView = headerView;
        this.referenceSequenceDictionaryView = referenceSequenceDictionaryView;
        this.BRAC = BRAC;
    }

    // This ctor leaves referenceSequenceDictionaryView uninitialized. Caveat emptor.
    private BaseRecalibratorFn(SAMFileHeader header, BaseRecalibrationArgumentCollection BRAC) {
        super("BaseRecalibratorFn");
        this.header = header;
        this.BRAC = BRAC;
    }

    // saves to BRAC.BRAC.RAC.RECAL_TABLE_FILE.getName()
    public static void SaveTextualReport(File output, SAMFileHeader header, RecalibrationTables rt, BaseRecalibrationArgumentCollection BRAC) throws IOException {
        QuantizationInfo qi = new QuantizationInfo(rt, BRAC.RAC.QUANTIZING_LEVELS);
        BaseRecalibratorFn worker = new BaseRecalibratorFn(header, BRAC);
        worker.onTraversalStart();
        worker.saveReport(output, rt, qi, worker.covariates);
    }

    @Override
    public void startBundle(Context c) throws Exception {
        super.startBundle(c);
        // can't set things up here because we don't have our side input yet.
        // So instead we're doing it in processElement.
        nReadsProcessed = 0;
        // resetting firstInBundle in case we have multiple bundles via the same instance.
        firstInBundle = true;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {

        // can't set things up in startBundle because it doesn't have our side input yet.
        // So instead we're doing it here in processElement.
        if (firstInBundle) {
            init(c);
            onTraversalStart();
            firstInBundle = false;
        }
        GATKRead r = c.element().getKey();
        ReadContextData rc = c.element().getValue();
        Iterable<Variant> variants = rc.getOverlappingVariants();
        final ReferenceBases refBases = rc.getOverlappingReferenceBases();
        ReferenceDataSource refDS = new ReferenceMemorySource(refBases, referenceSequenceDictionary);

        apply(r, refDS, variants);
    }

    @Override
    public void finishBundle(Context c) throws Exception {
        bunny.stepEnd("finishBundle");
        if (null!=recalibrationEngine) {
            final RecalibrationTables recalibrationTables = recalibrationEngine.getRecalibrationTables();
            if (null==recalibrationTables) {
                throw new GATKException.ShouldNeverReachHereException("null recalibrationTables");
            }
            c.output(recalibrationTables);
        } else {
            // this can happen, but only if we processed zero read in this bundle
            if (nReadsProcessed!=0) {
                throw new GATKException.ShouldNeverReachHereException("null==recalibrationEngine yet nReadsProcessed!=0");
            }
        }
        bunny.stepEnd("Processed "+nReadsProcessed+" reads.");
        super.finishBundle(c);
    }

    // -------------------------------------------------------------
    // helper functions (non-public)

    private void init(ProcessContext c) {
        header = c.sideInput(headerView);
        referenceSequenceDictionary = c.sideInput(referenceSequenceDictionaryView);
    }

    private void onTraversalStart() {

        baq = new BAQ(BRAC.BAQGOP); // setup the BAQ object with the provided gap open penalty

        if (BRAC.RAC.FORCE_PLATFORM != null)
            BRAC.RAC.DEFAULT_PLATFORM = BRAC.RAC.FORCE_PLATFORM;

        covariates = new StandardCovariateList(BRAC.RAC, header);

        int numReadGroups = header.getReadGroups().size();

        recalibrationEngine = new RecalibrationEngine(covariates, numReadGroups);

        minimumQToUse = BRAC.PRESERVE_QSCORES_LESS_THAN;


    }

    /**
     * For each read at this locus get the various covariate values and increment that location in the map based on
     * whether or not the base matches the reference at this particular location.
     * The "knownLocations" list doesn't need to be complete but it must includes all those that overlap with the read.
     *
     * Soft-clipped bases won't be considered in the error model.
     */
    private void apply(GATKRead originalRead, final ReferenceDataSource refDS, Iterable<? extends Locatable> knownLocations) {
        ReadTransformer transform = makeReadTransform();
        final GATKRead read = transform.apply(originalRead);

        if (read.isEmpty()) {
            return;
        } // the whole read was inside the adaptor so skip it

        RecalUtils.parsePlatformForRead(read, header, BRAC.RAC);
        if (!RecalUtils.isColorSpaceConsistent(BRAC.RAC.SOLID_NOCALL_STRATEGY, read, header)) {
            // parse the solid color space and check for color no-calls
            return; // skip this read completely
        }

        // We've checked in onTraversalStart() that we have a reference, so ref.get() is safe
        final int[] isSNP = calculateIsSNP(read, refDS);
        final int[] isInsertion = calculateIsIndel(read, EventType.BASE_INSERTION);
        final int[] isDeletion = calculateIsIndel(read, EventType.BASE_DELETION);
        final int nErrors = nEvents(isSNP, isInsertion, isDeletion);

        // note for efficiency regions we don't compute the BAQ array unless we actually have
        // some error to marginalize over.  For ILMN data ~85% of reads have no error
        final byte[] baqArray = nErrors == 0 ? flatBAQArray(read) : calculateBAQArray(read, refDS);

        if( baqArray != null ) { // some reads just can't be BAQ'ed
            final ReadCovariates covariates = RecalUtils.computeCovariates(read, header, this.covariates);
            final boolean[] skip = calculateSkipArray(read, knownLocations); // skip known sites of variation as well as low quality and non-regular bases
            final double[] snpErrors = calculateFractionalErrorArray(isSNP, baqArray);
            final double[] insertionErrors = calculateFractionalErrorArray(isInsertion, baqArray);
            final double[] deletionErrors = calculateFractionalErrorArray(isDeletion, baqArray);

            // aggregate all of the info into our info object, and update the data
            final ReadRecalibrationInfo info = new ReadRecalibrationInfo(read, covariates, skip, snpErrors, insertionErrors, deletionErrors);
            recalibrationEngine.updateDataForRead(info);
        }

        nReadsProcessed++;
    }

    /**
     * Compute the number of mutational events across all hasEvent vectors
     * <p>
     * Simply the sum of entries in hasEvents
     *
     * @param hasEvents a vector a vectors of 0 (no event) and 1 (has event)
     * @return the total number of events across all hasEvent arrays
     */
    protected static int nEvents(final int[]... hasEvents) {
        int n = 0;
        for (final int[] hasEvent : hasEvents) {
            n += MathUtils.sum(hasEvent);
        }
        return n;
    }

    private boolean[] calculateSkipArray(final GATKRead read, final Iterable<? extends Locatable> skipLocations) {
        final byte[] bases = read.getBases();
        final boolean[] skip = new boolean[bases.length];
        final boolean[] knownSites = calculateKnownSites(read, skipLocations);
        for (int iii = 0; iii < bases.length; iii++) {
            skip[iii] = !BaseUtils.isRegularBase(bases[iii]) || isLowQualityBase(read, iii) || knownSites[iii] || badSolidOffset(read, iii);
        }
        return skip;
    }

    private boolean[] calculateKnownSites(final GATKRead read, final Iterable<? extends Locatable> knownLocations) {
        final int readLength = read.getBases().length;
        final boolean[] knownSites = new boolean[readLength];
        Arrays.fill(knownSites, false);
        for (final Locatable feat : knownLocations) {
            int featureStartOnRead = ReadUtils.getReadCoordinateForReferenceCoordinate(ReadUtils.getSoftStart(read), read.getCigar(), feat.getStart(), ReadUtils.ClippingTail.LEFT_TAIL, true); // BUGBUG: should I use LEFT_TAIL here?
            if (featureStartOnRead == ReadUtils.CLIPPING_GOAL_NOT_REACHED) {
                featureStartOnRead = 0;
            }

            int featureEndOnRead = ReadUtils.getReadCoordinateForReferenceCoordinate(ReadUtils.getSoftStart(read), read.getCigar(), feat.getEnd(), ReadUtils.ClippingTail.LEFT_TAIL, true);
            if (featureEndOnRead == ReadUtils.CLIPPING_GOAL_NOT_REACHED) {
                featureEndOnRead = readLength;
            }

            if (featureStartOnRead > readLength) {
                featureStartOnRead = featureEndOnRead = readLength;
            }

            Arrays.fill(knownSites, Math.max(0, featureStartOnRead), Math.min(readLength, featureEndOnRead + 1), true);
        }
        return knownSites;
    }


    private boolean isLowQualityBase(final GATKRead read, final int offset) {
        return read.getBaseQualities()[offset] < minimumQToUse;
    }

    private boolean badSolidOffset(final GATKRead read, final int offset) {
        return ReadUtils.isSOLiDRead(read, header) && BRAC.RAC.SOLID_RECAL_MODE != RecalUtils.SOLID_RECAL_MODE.DO_NOTHING && !RecalUtils.isColorSpaceConsistent(read, offset);
    }

    // TODO: can be merged with calculateIsIndel
    private static int[] calculateIsSNP(final GATKRead read, final ReferenceDataSource ref) {
        final byte[] readBases = read.getBases();
        final byte[] refBases = ref.queryAndPrefetch(read.getContig(), read.getStart(), read.getEnd()).getBases();
        int refPos = 0;



        final int[] snp = new int[readBases.length];
        int readPos = 0;
        for (final CigarElement ce : read.getCigar().getCigarElements()) {
            final int elementLength = ce.getLength();
            switch (ce.getOperator()) {
                case M:
                case EQ:
                case X:
                    for (int iii = 0; iii < elementLength; iii++) {
                        snp[readPos] = (BaseUtils.basesAreEqual(readBases[readPos], refBases[refPos]) ? 0 : 1);
                        readPos++;
                        refPos++;
                    }
                    break;
                case D:
                case N:
                    refPos += elementLength;
                    break;
                case I:
                case S: // ReferenceContext doesn't have the soft clipped bases!
                    readPos += elementLength;
                    break;
                case H:
                case P:
                    break;
                default:
                    throw new GATKException("Unsupported cigar operator: " + ce.getOperator());
            }
        }
        return snp;
    }

    @VisibleForTesting
    static int[] calculateIsIndel(final GATKRead read, final EventType mode) {
        final int[] indel = new int[read.getBases().length];
        int readPos = 0;
        for (final CigarElement ce : read.getCigar().getCigarElements()) {
            final int elementLength = ce.getLength();
            switch (ce.getOperator()) {
                case M:
                case EQ:
                case X:
                case S: {
                    readPos += elementLength;
                    break;
                }
                case D: {
                    final int index = (read.isReverseStrand() ? readPos : readPos - 1);
                    updateIndel(indel, index, mode, EventType.BASE_DELETION);
                    break;
                }
                case I: {
                    final boolean forwardStrandRead = !read.isReverseStrand();
                    if (forwardStrandRead) {
                        updateIndel(indel, readPos - 1, mode, EventType.BASE_INSERTION);
                    }
                    readPos += elementLength;
                    if (!forwardStrandRead) {
                        updateIndel(indel, readPos, mode, EventType.BASE_INSERTION);
                    }
                    break;
                }
                case N:
                case H:
                case P:
                    break;
                default:
                    throw new GATKException("Unsupported cigar operator: " + ce.getOperator());
            }
        }
        return indel;
    }



    private static void updateIndel(final int[] indel, final int index, final EventType mode, final EventType requiredMode) {
        if (mode == requiredMode && index >= 0 && index < indel.length)
            // protect ourselves from events at the start or end of the read (1D3M or 3M1D)
            indel[index] = 1;
    }


    @VisibleForTesting
    static double[] calculateFractionalErrorArray(final int[] errorArray, final byte[] baqArray) {
        if (errorArray.length != baqArray.length) {
            throw new GATKException("Array length mismatch detected. Malformed read?");
        }

        final int BLOCK_START_UNSET = -1;

        final double[] fractionalErrors = new double[baqArray.length];
        Arrays.fill(fractionalErrors, 0.0);
        boolean inBlock = false;
        int blockStartIndex = BLOCK_START_UNSET;
        int iii;
        for (iii = 0; iii < fractionalErrors.length; iii++) {
            if (baqArray[iii] == NO_BAQ_UNCERTAINTY) {
                if (!inBlock) {
                    fractionalErrors[iii] = (double) errorArray[iii];
                } else {
                    calculateAndStoreErrorsInBlock(iii, blockStartIndex, errorArray, fractionalErrors);
                    inBlock = false; // reset state variables
                    blockStartIndex = BLOCK_START_UNSET; // reset state variables
                }
            } else {
                inBlock = true;
                if (blockStartIndex == BLOCK_START_UNSET) {
                    blockStartIndex = iii;
                }
            }
        }
        if (inBlock) {
            calculateAndStoreErrorsInBlock(iii - 1, blockStartIndex, errorArray, fractionalErrors);
        }
        if (fractionalErrors.length != errorArray.length) {
            throw new GATKException("Output array length mismatch detected. Malformed read?");
        }
        return fractionalErrors;
    }

    private static void calculateAndStoreErrorsInBlock(final int iii,
                                                       final int blockStartIndex,
                                                       final int[] errorArray,
                                                       final double[] fractionalErrors) {
        int totalErrors = 0;
        for (int jjj = Math.max(0, blockStartIndex - 1); jjj <= iii; jjj++) {
            totalErrors += errorArray[jjj];
        }
        for (int jjj = Math.max(0, blockStartIndex - 1); jjj <= iii; jjj++) {
            fractionalErrors[jjj] = ((double) totalErrors) / ((double) (iii - Math.max(0, blockStartIndex - 1) + 1));
        }
    }

    /**
     * Create a BAQ style array that indicates no alignment uncertainty
     *
     * @param read the read for which we want a BAQ array
     * @return a BAQ-style non-null byte[] counting NO_BAQ_UNCERTAINTY values
     * // TODO -- could be optimized avoiding this function entirely by using this inline if the calculation code above
     */
    private static byte[] flatBAQArray(final GATKRead read) {
        final byte[] baq = new byte[read.getLength()];
        Arrays.fill(baq, NO_BAQ_UNCERTAINTY);
        return baq;
    }

    /**
     * Compute an actual BAQ array for read, based on its quals and the reference sequence
     *
     * @param read the read to BAQ
     * @return a non-null BAQ tag array for read
     */
    private byte[] calculateBAQArray(final GATKRead read, ReferenceDataSource refDS) {
        baq.baqRead(read, refDS, BAQ.CalculationMode.RECALCULATE, BAQ.QualityMode.ADD_TAG);
        return BAQ.getBAQTag(read);
    }

    private ReadTransformer makeReadTransform() {
        ReadTransformer f0 = BaseRecalibratorFn::consolidateCigar;

        ReadTransformer f =
            f0.andThen(this::setDefaultBaseQualities)
                .andThen(this::resetOriginalBaseQualities)
                .andThen(ReadClipper::hardClipAdaptorSequence)
                .andThen(ReadClipper::hardClipSoftClippedBases);

        return f;
    }

    private static GATKRead consolidateCigar(GATKRead read) {
        // Always consolidate the cigar string into canonical form, collapsing zero-length / repeated cigar elements.
        // Downstream code cannot necessarily handle non-consolidated cigar strings.
        read.setCigar(AlignmentUtils.consolidateCigar(read.getCigar()));
        return read;
    }

    private GATKRead setDefaultBaseQualities(GATKRead read) {
        // if we are using default quals, check if we need them, and add if necessary.
        // 1. we need if reads are lacking or have incomplete quality scores
        // 2. we add if defaultBaseQualities has a positive value
        if (BRAC.defaultBaseQualities < 0) {
            return read;
        }
        byte reads[] = read.getBases();
        byte quals[] = read.getBaseQualities();
        if (quals == null || quals.length < reads.length) {
            byte new_quals[] = new byte[reads.length];
            Arrays.fill(new_quals, BRAC.defaultBaseQualities);
            read.setBaseQualities(new_quals);
        }
        return read;
    }

    private GATKRead resetOriginalBaseQualities(GATKRead read) {
        if (!BRAC.useOriginalBaseQualities) {
            return read;
        }
        return ReadUtils.resetOriginalBaseQualities(read);
    }

    private void saveReport(File output, RecalibrationTables rt, QuantizationInfo quantizationInfo, StandardCovariateList finalRequestedCovariates) throws FileNotFoundException {
        // luckily we don't use BRAC.RAC.RECAL_TABLE without first coming through here, so this is fine.
        BRAC.RAC.RECAL_TABLE = new PrintStream(output);
        RecalUtils.outputRecalibrationReport(BRAC.RAC, quantizationInfo, rt, finalRequestedCovariates, BRAC.RAC.SORT_BY_ALL_COLUMNS);
    }


}
