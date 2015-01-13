package org.broadinstitute.hellbender.tools;

import htsjdk.samtools.*;
import htsjdk.samtools.util.CloserUtil;
import htsjdk.samtools.util.IOUtil;
import org.apache.commons.io.FilenameUtils;
import org.broadinstitute.hellbender.cmdline.CommandLineProgram;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.Option;
import org.broadinstitute.hellbender.cmdline.StandardOptionDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadProgramGroup;
import org.broadinstitute.hellbender.tools.readersplitters.ReadGroupIdSplitter;
import org.broadinstitute.hellbender.tools.readersplitters.ReaderSplitter;
import org.broadinstitute.hellbender.tools.readersplitters.SampleNameSplitter;

import java.io.File;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@CommandLineProgramProperties(
        usage = "Outputs reads by read group, etc. " +
                "Not to be confused with a tool that splits reads for RNA-Seq analysis.",
        usageShort = "Outputs reads by read group, etc.",
        programGroup = ReadProgramGroup.class
)
public class SplitReads extends CommandLineProgram {

    public static final String SAMPLE_SHORT_NAME = "SM";
    public static final String READ_GROUP_SHORT_NAME = "RG";

    @Option(shortName = StandardOptionDefinitions.INPUT_SHORT_NAME,
            doc = "The SAM or BAM or CRAM file to be split.")
    public File INPUT;

    @Option(shortName = StandardOptionDefinitions.OUTPUT_SHORT_NAME,
            doc = "The directory to output SAM or BAM or CRAM files.")
    public File OUTPUT_DIRECTORY = new File("");

    @Option(shortName = SAMPLE_SHORT_NAME,
            doc = "Split file by sample.")
    public boolean SAMPLE;

    @Option(shortName = READ_GROUP_SHORT_NAME,
            doc = "Split file by read group.")
    public boolean READ_GROUP;

    @Override
    protected Object doWork() {
        splitReader();
        return null;
    }

    protected void splitReader() {
        IOUtil.assertFileIsReadable(INPUT);
        IOUtil.assertDirectoryIsWritable(OUTPUT_DIRECTORY);
        final SamReader in = SamReaderFactory.makeDefault().referenceSequence(REFERENCE_SEQUENCE).open(INPUT);
        final List<ReaderSplitter<?>> splitters = new ArrayList<>();
        if (SAMPLE) {
            splitters.add(new SampleNameSplitter());
        }
        if (READ_GROUP) {
            splitters.add(new ReadGroupIdSplitter());
        }
        Map<String, SAMFileWriter> outs = createWriters(in, splitters);
        for (final SAMRecord rec : in) {
            outs.get(getKey(splitters, rec)).addAlignment(rec);
        }
        CloserUtil.close(in);
        outs.values().forEach(CloserUtil::close);
    }

    /**
     * Creates SAMFileWriter instances for the reader splitters based on the input file.
     * @param in Input sam, bam, or cram.
     * @param splitters Reader splitters.
     * @return A map of file name keys to SAMFileWriter.
     */
    private Map<String, SAMFileWriter> createWriters(final SamReader in, final List<ReaderSplitter<?>> splitters) {
        final Map<String, SAMFileWriter> outs = new HashMap<>();

        final SAMFileWriterFactory samFileWriterFactory = new SAMFileWriterFactory();

        final SAMFileHeader samFileHeaderIn = in.getFileHeader();
        final String base = FilenameUtils.getBaseName(INPUT.getName());
        final String extension = "." + FilenameUtils.getExtension(INPUT.getName());

        // Build up a list of key options at each level.
        final List<List<?>> splitKeys = splitters.stream()
                .map(splitter -> splitter.getSplitsBy(samFileHeaderIn))
                .collect(Collectors.toList());

        // For every combination of keys, add a SAMFileWriter.
        addKey(splitKeys, 0, "", key -> {
            final SAMFileHeader samFileHeaderOut = samFileHeaderIn.clone();
            final File outFile = new File(OUTPUT_DIRECTORY, base + key + extension);
            outs.put(key, samFileWriterFactory.makeWriter(samFileHeaderOut, true, outFile, REFERENCE_SEQUENCE));
        });

        return outs;
    }

    /**
     * Recursively builds up a key, then when it reaches the bottom of the list, calls the adder on the generated key.
     * @param listKeys A outer list, where each inner list contains the output options for that level.
     * @param listIndex The current recursive index within the listKeys.
     * @param key The built up key recursively
     * @param adder Function to run on the recursively generated key once the bottom of the outer list is reached.
     */
    private void addKey(final List<List<?>> listKeys, final int listIndex,
                        final String key, final Consumer<String> adder) {
        if (listIndex < listKeys.size()) {
            for (final Object newKey : listKeys.get(listIndex)) {
                addKey(listKeys, listIndex + 1, key + "." + newKey, adder);
            }
        } else {
            adder.accept(key);
        }
    }

    /**
     * Traverses the splitters generating a key for this particular record.
     * @param splitters The list of splitters.
     * @param record The record to analyze.
     * @return The generated key that may then be used to find the appropriate SAMFileWriter.
     */
    private String getKey(final List<ReaderSplitter<?>> splitters, final SAMRecord record) {
        return splitters.stream()
                .map(s -> s.getSplitBy(record).toString())
                .reduce("", (acc, item) -> acc + "." + item);
    }
}
