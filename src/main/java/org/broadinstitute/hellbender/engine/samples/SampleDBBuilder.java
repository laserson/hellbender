package org.broadinstitute.hellbender.engine.samples;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import org.broadinstitute.hellbender.exceptions.UserException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

/**
 *
 */
public class SampleDBBuilder {
    PedigreeValidationType validationStrictness;
    final SampleDB sampleDB = new SampleDB();

    Set<Sample> samplesFromDataSources = new HashSet<Sample>();
    Set<Sample> samplesFromPedigrees = new HashSet<Sample>();

    public SampleDBBuilder(PedigreeValidationType validationStrictness) {
        this.validationStrictness = validationStrictness;
    }

    /**
     * Constructor takes both a SAM header and sample files because the two must be integrated.
     */

    // @TODO CMN - this was pulled from GATK ReadUtils - should it go somewhere else ?
    /**
     * Pull out the samples from a SAMFileHeader;
     * note that we use a TreeSet so that they are sorted
     *
     * @param header  the sam file header
     * @return list of strings representing the sample names
     */
    public static Set<String> getSAMFileSamples(final SAMFileHeader header) {
        // get all of the unique sample names
        final Set<String> samples = new TreeSet<String>();
        List<SAMReadGroupRecord> readGroups = header.getReadGroups();
        for (SAMReadGroupRecord readGroup : readGroups) {
            samples.add(readGroup.getSample());
        }
        return samples;
    }

    /**
     * Hallucinates sample objects for all the samples in the SAM file and stores them
     */
    public SampleDBBuilder addSamplesFromSAMHeader(final SAMFileHeader header) {
        addSamplesFromSampleNames(getSAMFileSamples(header));
        return this;
    }

    public SampleDBBuilder addSamplesFromSampleNames(final Collection<String> sampleNames) {
        for (final String sampleName : sampleNames) {
            if (sampleDB.getSample(sampleName) == null) {
                final Sample newSample = new Sample(sampleName, sampleDB);
                sampleDB.addSample(newSample);
                samplesFromDataSources.add(newSample); // keep track of data source samples
            }
        }
        return this;
    }

    public SampleDBBuilder addSamplesFromPedigreeFiles(final List<File> pedigreeFiles) {
        for (final File pedFile : pedigreeFiles) {
            Collection<Sample> samples = addSamplesFromPedigreeArgument(pedFile);
            samplesFromPedigrees.addAll(samples);
        }

        return this;
    }

    public SampleDBBuilder addSamplesFromPedigreeStrings(final List<String> pedigreeStrings) {
        for (final String pedString : pedigreeStrings) {
            Collection<Sample> samples = addSamplesFromPedigreeArgument(pedString);
            samplesFromPedigrees.addAll(samples);
        }

        return this;
    }

    /**
     * Parse one sample file and integrate it with samples that are already there
     * Fail quickly if we find any errors in the file
     */
    private Collection<Sample> addSamplesFromPedigreeArgument(File sampleFile) {
        final PedReader reader = new PedReader();

        try {
            return reader.parse(sampleFile, getMissingFields(sampleFile), sampleDB);
        } catch ( FileNotFoundException e ) {
            throw new UserException.CouldNotReadInputFile(sampleFile, e);
        }
    }

    private Collection<Sample> addSamplesFromPedigreeArgument(final String string) {
        final PedReader reader = new PedReader();
        return reader.parse(string, getMissingFields(string), sampleDB);
    }

    public SampleDB getFinalSampleDB() {
        validate();
        return sampleDB;
    }

    public EnumSet<PedReader.MissingPedField> getMissingFields(final Object engineArg) {
        // @TODO - CMN
        //if ( engine == null )
            return EnumSet.noneOf(PedReader.MissingPedField.class);
        //else {
        //    final List<String> posTags = engine.getTags(engineArg).getPositionalTags();
        //    return PedReader.parseMissingFieldTags(engineArg, posTags);
        //}
    }

    // --------------------------------------------------------------------------------
    //
    // Validation
    //
    // --------------------------------------------------------------------------------

    protected final void validate() {
        validatePedigreeIDUniqueness();
        if (validationStrictness != PedigreeValidationType.SILENT) {
            // check that samples in data sources are all annotated, if anything is annotated
            if (!samplesFromPedigrees.isEmpty() && ! samplesFromDataSources.isEmpty()) {
                final Set<String> sampleNamesFromPedigrees = new HashSet<String>();
                for (final Sample pSample : samplesFromPedigrees) {
                    sampleNamesFromPedigrees.add(pSample.getID());
                }

                for (final Sample dsSample : samplesFromDataSources)
                    if (!sampleNamesFromPedigrees.contains(dsSample.getID())) {
                        throw new UserException("Sample " + dsSample.getID()
                                + " found in data sources but not in pedigree files with STRICT pedigree validation");
                    }
            }
        }
    }

    private void validatePedigreeIDUniqueness() {
        final Set<String> pedigreeIDs = new HashSet<String>();
        for (Sample sample : samplesFromPedigrees ) {
            pedigreeIDs.add(sample.getID());
        }
        assert pedigreeIDs.size() == samplesFromPedigrees.size() : "The number of sample IDs extracted from the pedigree does not equal the number of samples in the pedigree. Is a sample associated with multiple families?";
    }
}
