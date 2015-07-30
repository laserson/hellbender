package org.broadinstitute.hellbender.tools.walkers.variantutils;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import htsjdk.tribble.Feature;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.variantcontext.VariantContextUtils;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFContigHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFStandardHeaderLines;
import htsjdk.variant.vcf.VCFUtils;

import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.VariantProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureInput;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadsContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.engine.filters.ExcludeIDsVariantFilter;
import org.broadinstitute.hellbender.engine.filters.IncludeIDsVariantFilter;
import org.broadinstitute.hellbender.engine.filters.IntervalVariantFilter;
import org.broadinstitute.hellbender.engine.filters.VariantFilter;
import org.broadinstitute.hellbender.engine.filters.VariantFilterLibrary;
import org.broadinstitute.hellbender.engine.filters.VariantTypesVariantFilter;
import org.broadinstitute.hellbender.engine.samples.MendelianViolation;
import org.broadinstitute.hellbender.engine.samples.PedigreeValidationType;
import org.broadinstitute.hellbender.engine.samples.SampleDB;
import org.broadinstitute.hellbender.engine.samples.SampleDBBuilder;
import org.broadinstitute.hellbender.engine.VariantWalker;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.commandline.HiddenOption;
import org.broadinstitute.hellbender.utils.io.ListFileUtils;
import org.broadinstitute.hellbender.utils.GenomeLoc;
import org.broadinstitute.hellbender.utils.GenomeLocParser;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.text.XReadLines;
import org.broadinstitute.hellbender.utils.variant.ChromosomeCountConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;
import org.broadinstitute.hellbender.utils.variant.GATKVariantContextUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

// @TODO: should the command lines below be removed from the javadoc ?
/**
 * Select a subset of variants from a larger callset
 *
 * <p>
 * Often, a VCF containing many samples and/or variants will need to be subset in order to facilitate certain analyses
 * (e.g. comparing and contrasting cases vs. controls; extracting variant or non-variant loci that meet certain
 * requirements, displaying just a few samples in a browser like IGV, etc.). SelectVariants can be used for this purpose.
 * </p>
 * <p>
 * There are many different options for selecting subsets of variants from a larger callset:
 * <ul>
 *     <li>Extract one or more samples from a callset based on either a complete sample name or a pattern match.</li>
 *     <li>Specify criteria for inclusion that place thresholds on annotation values, e.g. "DP > 1000" (depth of
 * coverage greater than 1000x), "AF < 0.25" (sites with allele frequency less than 0.25). These criteria are written
 * as "JEXL expressions", which are documented in the
 * <a href="http://www.broadinstitute.org/gatk/guide/article?id=1255">article about using JEXL expressions</a>.</li>
 *     <li>Provide concordance or discordance tracks in order to include or exclude variants that are
 * also present in other given callsets.</li>
 *     <li>Select variants based on criteria like their type
 * (e.g. INDELs only), evidence of mendelian violation, filtering status, allelicity, and so on.</li>
 * </ul>
 * </p>
 *
 * <p>There are also several options for recording the original values of certain annotations that are recalculated
 * when a subsetting the new callset, trimming alleles, and so on.</p>
 *
 * <h3>Input</h3>
 * <p>
 * A variant call set from which to select a subset.
 * </p>
 *
 * <h3>Output</h3>
 * <p>
 * A new VCF file containing the selected subset of variants.
 * </p>
 *
 * <h3>Usage examples</h3>
 * <h4>Select two samples out of a VCF with many samples</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -T SelectVariants \
 *   -R reference.fasta \
 *   -V input.vcf \
 *   -o output.vcf \
 *   -sn SAMPLE_A_PARC \
 *   -sn SAMPLE_B_ACTG
 * </pre>
 *
 * <h4>Select two samples and any sample that matches a regular expression</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -T SelectVariants \
 *   -R reference.fasta \
 *   -V input.vcf \
 *   -o output.vcf \
 *   -sn SAMPLE_1_PARC \
 *   -sn SAMPLE_1_ACTG \
 *   -se 'SAMPLE.+PARC'
 * </pre>
 *
 * <h4>Exclude two samples and any sample that matches a regular expression:</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -R ref.fasta \
 *   -T SelectVariants \
 *   --variant input.vcf \
 *   -o output.vcf \
 *   -xl_sn SAMPLE_1_PARC \
 *   -xl_sn SAMPLE_1_ACTG \
 *   -xl_se 'SAMPLE.+PARC'
 * </pre>
 *
 * <h4>Select any sample that matches a regular expression and sites where the QD annotation is more than 10:</h4>
 * <pre>
 * java -Xmx2g -jar GenomeAnalysisTK.jar \
 *   -R ref.fasta \
 *   -T SelectVariants \
 *   -R reference.fasta \
 *   -V input.vcf \
 *   -o output.vcf \
 *   -se 'SAMPLE.+PARC' \
 *   -select "QD > 10.0"
 * </pre>
 *
 * <h4>Select any sample that does not match a regular expression and sites where the QD annotation is more than 10:</h4>
 * <pre>
 * java  -jar GenomeAnalysisTK.jar \
 *   -R ref.fasta \
 *   -T SelectVariants \
 *   --variant input.vcf \
 *   -o output.vcf \
 *   -se 'SAMPLE.+PARC' \
 *   -select "QD > 10.0"
 *   -invertSelect
 * </pre>
 *
 * <h4>Select a sample and exclude non-variant loci and filtered loci (trim remaining alleles by default):</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -R ref.fasta \
 *   -T SelectVariants \
 *   -R reference.fasta \
 *   -V input.vcf \
 *   -o output.vcf \
 *   -sn SAMPLE_1_ACTG \
 *   -env \
 *   -ef
 * </pre>
 *
 * <h4>Select a sample, subset remaining alleles, but don't trim:</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -T SelectVariants \
 *   -R reference.fasta \
 *   -V input.vcf \
 *   -o output.vcf \
 *   -sn SAMPLE_1_ACTG \
 *   -env \
 *   -noTrim
 *</pre>
 *
 * <h4>Select a sample and restrict the output vcf to a set of intervals:</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -T SelectVariants \
 *   -R reference.fasta \
 *   -V input.vcf \
 *   -o output.vcf \
 *   -L /path/to/my.interval_list \
 *   -sn SAMPLE_1_ACTG
 * </pre>
 *
 * <h4>Select all calls missed in my vcf, but present in HapMap (useful to take a look at why these variants weren't called in my dataset):</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -T SelectVariants \
 *   -R reference.fasta \
 *   -V hapmap.vcf \
 *   --discordance myCalls.vcf \
 *   -o output.vcf \
 *   -sn mySample
 * </pre>
 *
 * <h4>Select all calls made by both myCalls and theirCalls (useful to take a look at what is consistent between two callers):</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -T SelectVariants \
 *   -R reference.fasta \
 *   -V myCalls.vcf \
 *   --concordance theirCalls.vcf \
 *   -o output.vcf \
 *   -sn mySample
 * </pre>
 *
 * <h4>Generating a VCF of all the variants that are mendelian violations. The optional argument '-mvq' restricts the selection to sites that have a QUAL score of 50 or more</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -T SelectVariants \
 *   -R reference.fasta \
 *   -V input.vcf \
 *   -ped family.ped \
 *   -mv -mvq 50 \
 *   -o violations.vcf
 * </pre>
 *
 * <h4>Generating a VCF of all the variants that are not mendelian violations. The optional argument '-mvq' together with '-invMv' restricts the selection to sites that have a QUAL score of 50 or less</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -T SelectVariants \
 *   -R reference.fasta \
 *   -V input.vcf \
 *   -ped family.ped \
 *   -mv -mvq 50 -invMv \
 *   -o violations.vcf
 * </pre>
 *
 * <h4>Create a set with 50% of the total number of variants in the variant VCF:</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -T SelectVariants \
 *   -R reference.fasta \
 *   -V input.vcf \
 *   -o output.vcf \
 *   -fraction 0.5
 * </pre>
 *
 * <h4>Select only indels between 2 and 5 bases long from a VCF:</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -R ref.fasta \
 *   -T SelectVariants \
 *   -R reference.fasta \
 *   -V input.vcf \
 *   -o output.vcf \
 *   -selectType INDEL
 *   -minIndelSize 2
 *   -maxIndelSize 5
 * </pre>
 *
 * <h4>Exclude indels from a VCF:</h4>
 * <pre>
 * java -Xmx2g -jar GenomeAnalysisTK.jar \
 *   -R ref.fasta \
 *   -T SelectVariants \
 *   --variant input.vcf \
 *   -o output.vcf \
 *   -selectTypeToExclude INDEL
 * </pre>
 *
 * <h4>Select only multi-allelic SNPs and MNPs from a VCF (i.e. SNPs with more than one allele listed in the ALT column):</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -T SelectVariants \
 *   -R reference.fasta \
 *   -V input.vcf \
 *   -o output.vcf \
 *   -selectType SNP -selectType MNP \
 *   -restrictAllelesTo MULTIALLELIC
 * </pre>
 *
 * <h4>Select IDs in fileKeep and exclude IDs in fileExclude:</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -R ref.fasta \
 *   -T SelectVariants \
 *   --variant input.vcf \
 *   -o output.vcf \
 *   -IDs fileKeep \
 *   -excludeIDs fileExclude
 * </pre>
 *
 * <h4>Select sites where there are between 2 and 5 samples and between 10 and 50 percent of the sample genotypes are filtered:</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -R ref.fasta \
 *   -T SelectVariants \
 *   --variant input.vcf \
 *   --maxFilteredGenotypes 5
 *   --minFilteredGenotypes 2
 *   --maxFractionFilteredGenotypes 0.60
 *   --minFractionFilteredGenotypes 0.10
 * </pre>
 *
 *  <h4>Set filtered genotypes to no-call (./.):</h4>
 * <pre>
 * java -jar GenomeAnalysisTK.jar \
 *   -R ref.fasta \
 *   -T SelectVariants \
 *   --variant input.vcf \
 *   --setFilteredGenotypesToNocall
 * </pre>
 *
 */
@CommandLineProgramProperties(
        summary = "Select variant calls based on sample names, patterns, etc.",
        oneLineSummary = "Select variants VCF",
        programGroup = VariantProgramGroup.class
)
public class SelectVariants extends VariantWalker {

    private static final int MAX_FILTERED_GENOTYPES_DEFAULT_VALUE  = Integer.MAX_VALUE;
    private static final int MIN_FILTERED_GENOTYPES_DEFAULT_VALUE  = 0;
    private static final double MAX_FRACTION_FILTERED_GENOTYPES_DEFAULT_VALUE = 1.0;
    private static final double MIN_FRACTION_FILTERED_GENOTYPES_DEFAULT_VALUE = 0.0;

    /**
     * A site is considered discordant if there exists some sample in the variant track that has a non-reference
     * genotype  and either the site isn't present in this track, the sample isn't present in this track, or the
     * sample is called reference in this track.
     */
    @Argument(fullName="discordance", shortName = "disc",
                    doc="Output variants not called in this comparison track", optional=true)
    private FeatureInput<VariantContext> discordanceTrack;

    /**
     * A site is considered concordant if (1) we are not looking for specific samples and there is a variant called
     * in both the variant and concordance tracks or (2) every sample present in the variant track is present in the
     * concordance track and they have the sample genotype call.
     */
    @Argument(fullName="concordance", shortName = "conc",
                    doc="Output variants also called in this comparison track", optional=true)
    private FeatureInput<VariantContext> concordanceTrack;

    @Argument(doc="File to which variants should be written")
    private File outFile = null;

    /**
     * This argument can be specified multiple times in order to provide multiple sample names.
     */
    @Argument(fullName="sample_name", shortName="sn", doc="Include genotypes from this sample", optional=true)
    private Set<String> sampleNames = new HashSet<>(0);

    /**
     * Using a regular expression allows you to match multiple sample names that have that pattern in common. This
     * argument can be specified multiple times in order to use multiple different matching patterns.
     */
    @Argument(fullName="sample_expressions", shortName="se",
                    doc="Regular expression to select multiple samples", optional=true)
    private Set<String> sampleExpressions = new HashSet<>(0);

    /**
     * Sample names should be in a plain text file listing one sample name per line. This argument can be specified
     * multiple times in order to provide multiple sample list files.
     */
    @Argument(fullName="sample_file", shortName="sf", doc="File containing a list of samples to include", optional=true)
    private Set<File> sampleFiles = new HashSet<>(0);

    /**
     * Note that sample exclusion takes precedence over inclusion, so that if a sample is in both lists it will be
     * excluded. This argument can be specified multiple times in order to provide multiple sample names.
     */
    @Argument(fullName="exclude_sample_name", shortName="xl_sn", doc="Exclude genotypes from this sample", optional=true)
    private Set<String> XLsampleNames = new HashSet<>(0);

    /**
     * Sample names should be in a plain text file listing one sample name per line. Note that sample exclusion takes
     * precedence over inclusion, so that if a sample is in both lists it will be excluded. This argument can be
     * specified multiple times in order to provide multiple sample list files.
     */
    @Argument(fullName="exclude_sample_file", shortName="xl_sf", doc="List of samples to exclude", optional=true)
    private Set<File> XLsampleFiles = new HashSet<>(0);

    /**
     * Using a regular expression allows you to match multiple sample names that have that pattern in common. Note that
     * sample exclusion takes precedence over inclusion, so that if a sample is in both lists it will be excluded. This
     * argument can be specified multiple times in order to use multiple different matching patterns.
     */
    @Argument(fullName="exclude_sample_expressions", shortName="xl_se",
                    doc="List of sample expressions to exclude", optional=true)
    private Set<String> XLsampleExpressions = new HashSet<>(0);

    /**
     * See example commands above for detailed usage examples. Note that these expressions are evaluated *after* the
     * specified samples are extracted and the INFO field annotations are updated.
     */
    @Argument(shortName="select", doc="One or more criteria to use when selecting the data", optional=true)
    private ArrayList<String> selectExpressions = new ArrayList<>();

    /**
     * Invert the selection criteria for -select.
     */
    @Argument(shortName="invertSelect", doc="Invert the selection criteria for -select", optional=true)
    private boolean invertSelect = false;

    /*
     * If this flag is enabled, sites that are found to be non-variant after the subsetting procedure (i.e. where none
     * of the selected samples display evidence of variation) will be excluded from the output.
     */
    @Argument(fullName="excludeNonVariants", shortName="env", doc="Don't include non-variant sites", optional=true)
    private boolean XLnonVariants = false;

    /**
     * If this flag is enabled, sites that have been marked as filtered (i.e. have anything other than `.` or `PASS`
     * in the FILTER field) will be excluded from the output.
     */
    @Argument(fullName="excludeFiltered", shortName="ef", doc="Don't include filtered sites", optional=true)
    private boolean XLfiltered = false;

    /**
     * The default behavior of this tool is to remove bases common to all remaining alleles after subsetting
     * operations have been completed, leaving only their minimal representation. If this flag is enabled, the original
     * alleles will be preserved as recorded in the input VCF.
     */
    @Argument(fullName="preserveAlleles", shortName="noTrim", doc="Preserve original alleles, do not trim", optional=true)
    private boolean preserveAlleles = false;

    /**
     * When this flag is enabled, all alternate alleles that are not present in the (output) samples will be removed.
     * Note that this even extends to biallelic SNPs - if the alternate allele is not present in any sample, it will be
     * removed and the record will contain a '.' in the ALT column. Note also that sites-only VCFs, by definition, do
     * not include the alternate allele in any genotype calls.
     */
    @Argument(fullName="removeUnusedAlternates", shortName="trimAlternates",
                    doc="Remove alternate alleles not present in any genotypes", optional=true)
    private boolean removeUnusedAlternates = false;

    /**
     * When this argument is used, we can choose to include only multiallelic or biallelic sites, depending on how many
     * alleles are listed in the ALT column of a VCF. For example, a multiallelic record such as:
     *     1    100 .   A   AAA,AAAAA
     * will be excluded if `-restrictAllelesTo BIALLELIC` is used, because there are two alternate alleles, whereas a
     * record such as:
     *     1    100 .   A  T
     * will be included in that case, but would be excluded if `-restrictAllelesTo MULTIALLELIC` is used.
     * Valid options are ALL (default), MULTIALLELIC or BIALLELIC.
     */
    @Argument(fullName="restrictAllelesTo", shortName="restrictAllelesTo",
                    doc="Select only variants of a particular allelicity", optional=true)
    private  NumberAlleleRestriction alleleRestriction = NumberAlleleRestriction.ALL;

    /**
     * When subsetting a callset, this tool recalculates the AC, AF, and AN values corresponding to the contents of the
     * subset. If this flag is enabled, the original values of those annotations will be stored in new annotations called
     * AC_Orig, AF_Orig, and AN_Orig.
     */
    @Argument(fullName="keepOriginalAC", shortName="keepOriginalAC",
                    doc="Store the original AC, AF, and AN values after subsetting", optional=true)
    private boolean keepOriginalChrCounts = false;

    /**
     * When subsetting a callset, this tool recalculates the site-level (INFO field) DP value corresponding to the
     * contents of the subset. If this flag is enabled, the original value of the DP annotation will be stored in
     * a new annotation called DP_Orig.
     */
    @Argument(fullName="keepOriginalDP", shortName="keepOriginalDP",
                    doc="Store the original DP value after subsetting", optional=true)
    private boolean keepOriginalDepth = false;

    /**
     * If this flag is enabled, this tool will select only variants that correspond to a mendelian violation as
     * determined on the basis of family structure. Requires passing a pedigree file using the engine-level
     * `-ped` argument.
     */
    @Argument(fullName="mendelianViolation", shortName="mv", doc="Output mendelian violation sites only", optional=true)
    private Boolean mendelianViolations = false;

    /**
     * If this flag is enabled, this tool will select only variants that do not correspond to a mendelian violation as
     * determined on the basis of family structure. Requires passing a pedigree file using the engine-level
     * `-ped` argument.
     */
    @Argument(fullName="invertMendelianViolation", shortName="invMv",
                    doc="Output non-mendelian violation sites only", optional=true)
    private Boolean invertMendelianViolations = false;

    /**
     * This argument specifies the genotype quality (GQ) threshold that all members of a trio must have in order
     * for a site to be accepted as a mendelian violation. Note that the `-mv` flag must be set for this argument
     * to have an effect.
     */
    @Argument(fullName="mendelianViolationQualThreshold", shortName="mvq",
                    doc="Minimum GQ score for each trio member to accept a site as a violation", optional=true)
    private double medelianViolationQualThreshold = 0;

    @Argument(fullName="pedigree", shortName="ped", doc="Pedigree file", optional=true)
    private File pedigreeFile = null;

    /**
     * The value of this argument should be a number between 0 and 1 specifying the fraction of total variants to be
     * randomly selected from the input callset. Note that this is done using a probabilistic function, so the final
     * result is not guaranteed to carry the exact fraction requested. Can be used for large fractions.
     */
    @Argument(fullName="select_random_fraction", shortName="fraction",
                    doc="Select a fraction of variants at random from the input", optional=true)
    private double fractionRandom = 0;

    /**
     * The value of this argument should be a number between 0 and 1 specifying the fraction of total variants to be
     * randomly selected from the input callset and set to no-call (./). Note that this is done using a probabilistic
     * function, so the final result is not guaranteed to carry the exact fraction requested. Can be used for large fractions.
     */
    @Argument(fullName="remove_fraction_genotypes", shortName="fractionGenotypes",
                        doc="Select a fraction of genotypes at random from the input and sets them to no-call", optional=true)
    private double fractionGenotypes = 0;

    /**
     * This argument selects particular kinds of variants out of a list. If left empty, there is no type selection
     * and all variant types are considered for other selection criteria. Valid types are INDEL, SNP, MIXED, MNP,
     * SYMBOLIC, NO_VARIATION. Can be specified multiple times.
     */
    @Argument(fullName="selectTypeToInclude", shortName="selectType",
                    doc="Select only a certain type of variants from the input file", optional=true)
    private List<VariantContext.Type> typesToInclude = new ArrayList<>();

    /**
     * This argument excludes particular kinds of variants out of a list. If left empty, there is no type selection
     * and all variant types are considered for other selection criteria. Valid types are INDEL, SNP, MIXED, MNP,
     * SYMBOLIC, NO_VARIATION. Can be specified multiple times.
     */
    @Argument(fullName="selectTypeToExclude", shortName="xlSelectType",
                    doc="Do not select certain type of variants from the input file", optional=true)
    private List<VariantContext.Type> typesToExclude = new ArrayList<>();

    /**
     * If a file containing a list of IDs is provided to this argument, the tool will only select variants whose ID
     * field is present in this list of IDs. The matching is done by exact string matching. The expected file format
     * is simply plain text with one ID per line.
     */
    @Argument(fullName="keepIDs", shortName="IDs", doc="List of variant IDs to select", optional=true)
    private File rsIDFile = null;

    /**
     * If a file containing a list of IDs is provided to this argument, the tool will not select variants whose ID
     * field is present in this list of IDs. The matching is done by exact string matching. The expected file format
     * is simply plain text with one ID per line.
     */
    @Argument(fullName="excludeIDs", shortName="xlIDs", doc="List of variant IDs to select", optional=true)
    private File XLrsIDFile = null;

    @HiddenOption
    @Argument(fullName="fullyDecode", doc="If true, the incoming VariantContext will be fully decoded", optional=true)
    private boolean fullyDecode = false;

    @HiddenOption
    @Argument(fullName="justRead",
                    doc="If true, we won't actually write the output file.  For efficiency testing only", optional=true)
    private boolean justRead = false;

    /**
     * If this argument is provided, indels that are larger than the specified size will be excluded.
     */
    @Argument(fullName="maxIndelSize", optional=true, doc="Maximum size of indels to include")
    private int maxIndelSize = Integer.MAX_VALUE;

    /**
     * If this argument is provided, indels that are smaller than the specified size will be excluded.
     */
    @Argument(fullName="minIndelSize", optional=true, doc="Minimum size of indels to include")
    private int minIndelSize = 0;

    /**
     * If this argument is provided, select sites where at most a maximum number of samples are filtered at the genotype level.
     */
    @Argument(fullName="maxFilteredGenotypes", optional=true, doc="Maximum number of samples filtered at the genotype level")
    private int maxFilteredGenotypes = MAX_FILTERED_GENOTYPES_DEFAULT_VALUE;

    /**
     * If this argument is provided, select sites where at least a minimum number of samples are filtered at
     * the genotype level.
     */
    @Argument(fullName="minFilteredGenotypes", optional=true,
                    doc="Minimum number of samples filtered at the genotype level")
    private int minFilteredGenotypes = MIN_FILTERED_GENOTYPES_DEFAULT_VALUE;

    /**
     * If this argument is provided, select sites where a fraction or less of the samples are filtered at
     * the genotype level.
     */
    @Argument(fullName="maxFractionFilteredGenotypes",
                    optional=true, doc="Maximum fraction of samples filtered at the genotype level")
    private double maxFractionFilteredGenotypes = MAX_FRACTION_FILTERED_GENOTYPES_DEFAULT_VALUE;

    /**
     * If this argument is provided, select sites where a fraction or more of the samples are filtered at
     * the genotype level.
     */
    @Argument(fullName="minFractionFilteredGenotypes", optional=true,
                    doc="Maximum fraction of samples filtered at the genotype level")
    private double minFractionFilteredGenotypes = MIN_FRACTION_FILTERED_GENOTYPES_DEFAULT_VALUE;

    /**
     * If this argument is provided, set filtered genotypes to no-call (./.).
     */
    @Argument(fullName="setFilteredGtToNocall", optional=true, doc="Set filtered genotypes to no-call")
    private boolean setFilteredGenotypesToNocall = false;

    @Argument(fullName=StandardArgumentDefinitions.LENIENT_LONG_NAME,
                            shortName = StandardArgumentDefinitions.LENIENT_SHORT_NAME,
                            doc = "Lenient processing of VCF files", common = true, optional = true)
    private boolean lenientVCFProcesssing;

    @Argument(fullName=StandardArgumentDefinitions.INTERVAL_LONG_NAME,
                    shortName = StandardArgumentDefinitions.INTERVAL_SHORT_NAME,
                    doc = "One or more genomic intervals over which to operate", common = true, optional = true)
    private String intervalList = null;

    @HiddenOption
    @Argument(fullName="ALLOW_NONOVERLAPPING_COMMAND_LINE_SAMPLES", optional=true,
                    doc="Allow samples other than those in the VCF to be specified on the command line. These samples will be ignored.")
    private boolean allowNonOverlappingCommandLineSamples = false;

    @HiddenOption
    @Argument(fullName="SUPPRESS_REFERENCE_NAME", shortName="sr", optional=true,
            doc="Suppress reference file name in output for test result differencing")
    private boolean suppressReferenceName = false;

    private VariantContextWriter vcfWriter = null;

    // intervals to process
    private List<GenomeLoc> genomeLocs =  null;

    private enum NumberAlleleRestriction {
        ALL,
        BIALLELIC,
        MULTIALLELIC
    }

    private TreeSet<String> samples = new TreeSet<>();
    private boolean noSamplesSpecified = false;

    private Set<VariantContext.Type> selectedTypes = new HashSet<>();
    private ArrayList<String> selectNames = new ArrayList<>();
    private List<VariantContextUtils.JexlVCMatchExp> jexls = null;

    private boolean discordanceOnly = false;
    private boolean concordanceOnly = false;

    private MendelianViolation mv = null;
    private SampleDB sampleDB = null;

    /* variables used by the SELECT RANDOM modules */
    private boolean selectRandomFraction = false;

    // Random number generator for the genotypes to remove
    private Random randomGenotypes = new Random();

    private Set<String> IDsToKeep = null;
    private Set<String> IDsToRemove = null;

    private final List<Allele> diploidNoCallAlleles = Arrays.asList(Allele.NO_CALL, Allele.NO_CALL);

    /**
     * Set up the VCF writer, the sample expressions and regexs, filters inputs, and the JEXL matcher
     *
     */
    @Override
    public void onTraversalStart() {
        final FeatureInput<? extends Feature> featureInput = getDrivingVariantsFeatureInput();

        final Map<String, VCFHeader> vcfRods = new HashMap<String, VCFHeader>();
        VCFHeader vcfHeader= getHeaderForVariants();
        vcfRods.put(featureInput.getName(), vcfHeader);

        // Initialize VCF header lines
        final Set<VCFHeaderLine> headerLines = createVCFHeaderLineList(vcfRods);

        for (int i = 0; i < selectExpressions.size(); i++) {
            // It's not necessary that the user supply select names for the JEXL expressions, since those
            // expressions will only be needed for omitting records.  Make up the select names here.
            selectNames.add(String.format("select-%d", i));
        }

        jexls = VariantContextUtils.initializeMatchExps(selectNames, selectExpressions);

        // Prepare the sample names and types to be used by the corresponding filters
        samples = createSampleNameInclusionList(vcfRods);
        selectedTypes = createSampleTypeInclusionList();

        // Look at the parameters to decide which analysis to perform
        discordanceOnly = discordanceTrack != null;
        if (discordanceOnly) {
            logger.info("Selecting only variants discordant with the track: " + discordanceTrack.getName());
        }

        concordanceOnly = concordanceTrack != null;
        if (concordanceOnly) {
            logger.info("Selecting only variants concordant with the track: " + concordanceTrack.getName());
        }

        if (mendelianViolations) {
            sampleDB = initializeSampleDB();
            mv = new MendelianViolation(medelianViolationQualThreshold,false,true);
        }

        selectRandomFraction = fractionRandom > 0;
        if (selectRandomFraction) {
            logger.info("Selecting approximately " + 100.0*fractionRandom + "% of the variants at random from the variant track");
        }

        // Prepare the variant IDs to keep and to be removed for use by the IDs filter
        IDsToKeep = getIDsFromFile(rsIDFile);
        IDsToRemove = getIDsFromFile(XLrsIDFile);

        // Prepare the interval list for use by the interval filter
        if (intervalList != null) {
            ReferenceSequenceFile rsf = ReferenceSequenceFileFactory.getReferenceSequenceFile(referenceArguments.getReferenceFile());
            genomeLocs = IntervalUtils.parseIntervalArguments(new GenomeLocParser(rsf), intervalList);
        }

        VariantContextWriterBuilder vcWriterBuilder =
                new VariantContextWriterBuilder()
                    .setOutputFile(outFile)
                    .setOutputFileType(VariantContextWriterBuilder.OutputType.VCF)
                    //.unsetOption(Options.WRITE_FULL_FORMAT_FIELD)
                    .unsetOption(Options.INDEX_ON_THE_FLY);
        if (lenientVCFProcesssing) {
            vcWriterBuilder = vcWriterBuilder.setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER);
        }
        vcfWriter = vcWriterBuilder.build();

        final File refFile = referenceArguments.getReferenceFile();
        SAMSequenceDictionary samDictionary = this.getBestAvailableSequenceDictionary();
        Set<VCFHeaderLine> actualLines = null;
        if (refFile != null && samDictionary!= null) {
            actualLines = withUpdatedContigsAsLines(headerLines, refFile, samDictionary, suppressReferenceName);
        }
        else {
            actualLines = headerLines;
        }
        vcfWriter.writeHeader(new VCFHeader(actualLines, samples));
    }

    @Override
    public void apply(VariantContext vc, ReadsContext readsContext, ReferenceContext ref, FeatureContext featureContext) {

        if (mendelianViolations && invertLogic(mv.countViolations(sampleDB.getFamilies(samples), vc) == 0, invertMendelianViolations)) {
            return;
        }

        if (discordanceOnly && !isDiscordant(vc, featureContext.getValues(discordanceTrack, 0, 0))) {
            return;
        }

        if (concordanceOnly && !isConcordant(vc, featureContext.getValues(concordanceTrack, 0, 0))) {
            return;
        }

        // TODO: Some of these tests could factored out into re-useable filter predicates if they're useful in other tools
        if (alleleRestriction.equals(NumberAlleleRestriction.BIALLELIC) && !vc.isBiallelic()) {
            return;
        }

        if (alleleRestriction.equals(NumberAlleleRestriction.MULTIALLELIC) && vc.isBiallelic()) {
            return;
        }

        if (containsIndelLargerOrSmallerThan(vc, maxIndelSize, minIndelSize)) {
            return;
        }

        if (needNumFilteredGenotypes()) {
            int numFilteredSamples = numFilteredGenotypes(vc);
            double fractionFilteredGenotypes = samples.isEmpty() ? 0.0 : numFilteredSamples / samples.size();
            if (numFilteredSamples > maxFilteredGenotypes || numFilteredSamples < minFilteredGenotypes ||
                    fractionFilteredGenotypes > maxFractionFilteredGenotypes || fractionFilteredGenotypes < minFractionFilteredGenotypes)
                return;
        }

        VariantContext sub = subsetRecord(vc, preserveAlleles, removeUnusedAlternates);
        VariantContext filteredGenotypeToNocall = setFilteredGenotypeToNocall(sub, setFilteredGenotypesToNocall);

        // Not excluding non-variants or subsetted polymorphic variants AND including filtered loci or subsetted variant is not filtered
        if ((!XLnonVariants || filteredGenotypeToNocall.isPolymorphicInSamples()) && (!XLfiltered || !filteredGenotypeToNocall.isFiltered())) {

            // Write the subsetted variant if it matches all of the expressions
            boolean failedJexlMatch = false;

            try {
                for (VariantContextUtils.JexlVCMatchExp jexl : jexls) {
                    if (invertLogic(!VariantContextUtils.match(filteredGenotypeToNocall, jexl), invertSelect)){
                        failedJexlMatch = true;
                        break;
                    }
                }
            } catch (IllegalArgumentException e) {
                //The IAE thrown by htsjdk already includes an informative error message ("Invalid JEXL
                //  expression detected...")
                throw new UserException(e.getMessage());
            }

            if (!failedJexlMatch &&
                    !justRead &&
                    (!selectRandomFraction || Utils.getRandomGenerator().nextDouble() < fractionRandom)) {
                vcfWriter.add(filteredGenotypeToNocall);
            }
        }
    }

    /**
     * Close out the new variants file.
     */
    @Override
    public Object onTraversalDone() {
        try {
            return null;
        } finally {
            vcfWriter.close();
        }
    }

    /**
     * Create filters for variant types, ids, and genomic intervals.
     */
    protected VariantFilter makeVariantFilter() {
        VariantFilter compositeFilter = null;
        VariantFilter tmpFilter = null;

        if (selectedTypes.size() != 0) {
            tmpFilter = new VariantTypesVariantFilter(selectedTypes);
            compositeFilter = compositeFilter == null ? tmpFilter : compositeFilter.and(tmpFilter);
        }

        if (IDsToKeep != null && IDsToKeep.size() > 0) {
            tmpFilter = new IncludeIDsVariantFilter(IDsToKeep);
            compositeFilter = compositeFilter == null ? tmpFilter : compositeFilter.and(tmpFilter);
        }

        if (IDsToRemove != null && IDsToRemove.size() > 0) {
            tmpFilter = new ExcludeIDsVariantFilter(IDsToRemove);
            compositeFilter = compositeFilter == null ? tmpFilter : compositeFilter.and(tmpFilter);
        }

        if (genomeLocs != null && genomeLocs.size() > 0) {
            tmpFilter = new IntervalVariantFilter(genomeLocs);
            compositeFilter = compositeFilter == null ? tmpFilter : compositeFilter.and(tmpFilter);
        }

        return compositeFilter != null ? compositeFilter : VariantFilterLibrary.ALLOW_ALL_VARIANTS;
    }

    /**
     * Prepare the sample names to be included(/excluded) in the output by the names filter.
     */
    private TreeSet<String> createSampleNameInclusionList(Map<String, VCFHeader> vcfHeaders) {
        final TreeSet<String> vcfSamples = new TreeSet<>(getSampleList(vcfHeaders, GATKVariantContextUtils.GenotypeMergeType.REQUIRE_UNIQUE));
        final Collection<String> samplesFromFile = getSamplesFromFiles(sampleFiles);
        final Collection<String> samplesFromExpressions = matchSamplesExpressions(vcfSamples, sampleExpressions);

        // first, check overlap between requested and present samples
        final Set<String> commandLineUniqueSamples = new HashSet<>(samplesFromFile.size()+samplesFromExpressions.size()+sampleNames.size());
        commandLineUniqueSamples.addAll(samplesFromFile);
        commandLineUniqueSamples.addAll(samplesFromExpressions);
        commandLineUniqueSamples.addAll(sampleNames);
        commandLineUniqueSamples.removeAll(vcfSamples);

        // second, add the requested samples
        samples.addAll(sampleNames);
        samples.addAll(samplesFromExpressions);
        samples.addAll(samplesFromFile);

        logger.debug(Utils.join(",", commandLineUniqueSamples));

        if (!commandLineUniqueSamples.isEmpty()) {
            if (allowNonOverlappingCommandLineSamples) {
                logger.warn("Samples present on command line input that are not present in the VCF. These samples will be ignored.");
                samples.removeAll(commandLineUniqueSamples);
            } else {
                throw new UserException.BadInput(String.format("%s%n%n%s%n%n%s%n%n%s",
                        "Samples entered on command line (through -sf or -sn) that are not present in the VCF.",
                        "A list of these samples:",
                        Utils.join(",", commandLineUniqueSamples),
                        "To ignore these samples, run with --allowNonOverlappingCommandLineSamples"));
            }
        }

        // if none were requested, we want all of them
        if (samples.isEmpty()) {
            samples.addAll(vcfSamples);
            noSamplesSpecified = true;
        }

        // Exclude samples take precedence over include - remove any excluded samples
        final Collection<String> XLsamplesFromFile = getSamplesFromFiles(XLsampleFiles);
        final Collection<String> XLsamplesFromExpressions = matchSamplesExpressions(vcfSamples, XLsampleExpressions);
        samples.removeAll(XLsamplesFromFile);
        samples.removeAll(XLsampleNames);
        samples.removeAll(XLsamplesFromExpressions);
        noSamplesSpecified = noSamplesSpecified && XLsampleNames.isEmpty() && XLsamplesFromFile.isEmpty() &&
                XLsamplesFromExpressions.isEmpty();

        if (samples.isEmpty() && !noSamplesSpecified)
            throw new UserException("All samples requested to be included were also requested to be excluded.");

        if (!noSamplesSpecified)
            for (String sample : samples)
                logger.info("Including sample '" + sample + "'");

        return samples;
    }

    /**
     * Prepare the type inclusion list to be used by the type filter
     */
    private Set<VariantContext.Type> createSampleTypeInclusionList() {

        // if user specified types to include, add these, otherwise, add all possible variant context types to list of vc types to include
        if (typesToInclude.isEmpty()) {
            for (VariantContext.Type t : VariantContext.Type.values())
                selectedTypes.add(t);
        } else {
            for (VariantContext.Type t : typesToInclude)
                selectedTypes.add(t);
        }

        // Exclude types take precedence over inlcude - remove specified exclude types
        for (VariantContext.Type t : typesToExclude)
            selectedTypes.remove(t);

        return selectedTypes;
    }

    /**
     * Prepare the VCF header lines
     */
    private Set<VCFHeaderLine> createVCFHeaderLineList(Map<String, VCFHeader> vcfHeaders) {

        final Set<VCFHeaderLine> headerLines = VCFUtils.smartMergeHeaders(vcfHeaders.values(), true);
        headerLines.add(new VCFHeaderLine("source", "SelectVariants"));

        if (keepOriginalChrCounts) {
            headerLines.add(GATKVCFHeaderLines.getInfoLine(GATKVCFConstants.ORIGINAL_AC_KEY));
            headerLines.add(GATKVCFHeaderLines.getInfoLine(GATKVCFConstants.ORIGINAL_AF_KEY));
            headerLines.add(GATKVCFHeaderLines.getInfoLine(GATKVCFConstants.ORIGINAL_AN_KEY));
        }
        if (keepOriginalDepth)
            headerLines.add(GATKVCFHeaderLines.getInfoLine(GATKVCFConstants.ORIGINAL_DP_KEY));

        headerLines.addAll(Arrays.asList(ChromosomeCountConstants.descriptions));
        headerLines.add(VCFStandardHeaderLines.getInfoLine(VCFConstants.DEPTH_KEY));

        return headerLines;
    }

    private static Set<VCFHeaderLine> withUpdatedContigsAsLines(
            final Set<VCFHeaderLine> oldLines,
            final File referenceFile,
            final SAMSequenceDictionary refDict,
            final boolean referenceNameOnly) {
        final Set<VCFHeaderLine> lines = new LinkedHashSet<VCFHeaderLine>(oldLines.size());

        for (final VCFHeaderLine line : oldLines) {
            if (line instanceof VCFContigHeaderLine)
                continue; // skip old contig lines
            if (line.getKey().equals(VCFHeader.REFERENCE_KEY))
                continue; // skip the old reference key
            lines.add(line);
        }

        for (final VCFHeaderLine contigLine : makeContigHeaderLines(refDict, referenceFile))
            lines.add(contigLine);

        final String referenceValue;
        if (referenceFile != null) {
            if (referenceNameOnly) {
                final int extensionStart = referenceFile.getName().lastIndexOf(".");
                referenceValue = extensionStart == -1 ? referenceFile.getName() : referenceFile.getName().substring(0, extensionStart);
            }
            else {
                referenceValue = "file://" + referenceFile.getAbsolutePath();
            }
            lines.add(new VCFHeaderLine(VCFHeader.REFERENCE_KEY, referenceValue));
        }
        return lines;
    }

    /**
     * Create VCFHeaderLines for each refDict entry, and optionally the assembly if referenceFile != null
     * @param refDict reference dictionary
     * @param referenceFile for assembly name.  May be null
     * @return list of vcf contig header lines
     */
    public static List<VCFContigHeaderLine> makeContigHeaderLines(final SAMSequenceDictionary refDict,
                                                                  final File referenceFile) {
        final List<VCFContigHeaderLine> lines = new ArrayList<VCFContigHeaderLine>();
        final String assembly = referenceFile != null ? getReferenceAssembly(referenceFile.getName()) : null;
        for (final SAMSequenceRecord contig : refDict.getSequences())
            lines.add(makeContigHeaderLine(contig, assembly));
        return lines;
    }

    private static VCFContigHeaderLine makeContigHeaderLine(final SAMSequenceRecord contig, final String assembly) {
        final Map<String, String> map = new LinkedHashMap<String, String>(3);
        map.put("ID", contig.getSequenceName());
        map.put("length", String.valueOf(contig.getSequenceLength()));
        if (assembly != null) {
            map.put("assembly", assembly);
        }
        return new VCFContigHeaderLine(map, contig.getSequenceIndex());
    }

    /*
     * TODO: the GATK code does this to include in the headers - seems pretty sketchy...
     */
    private static String getReferenceAssembly(final String refPath) {
        // This doesn't need to be perfect as it's not a required VCF header line, but we might as well give it a shot
        String assembly = null;
        if (refPath.contains("b37") || refPath.contains("v37"))
            assembly = "b37";
        else if (refPath.contains("b36"))
            assembly = "b36";
        else if (refPath.contains("hg18"))
            assembly = "hg18";
        else if (refPath.contains("hg19"))
            assembly = "hg19";
        return assembly;
    }

    /**
     * Entry-point function to initialize the samples database from input data
     */
    private SampleDB initializeSampleDB() {
        final SampleDBBuilder sampleDBBuilder = new SampleDBBuilder(PedigreeValidationType.STRICT);
        sampleDBBuilder.addSamplesFromPedigreeFiles(Arrays.asList(pedigreeFile));
        return sampleDBBuilder.getFinalSampleDB();
    }


    public static Set<String> getSampleList(Map<String, VCFHeader> headers, GATKVariantContextUtils.GenotypeMergeType mergeOption) {
        final Set<String> samples = new TreeSet<String>();
        for (final Map.Entry<String, VCFHeader> val : headers.entrySet()) {
            VCFHeader header = val.getValue();
            for (final String sample : header.getGenotypeSamples()) {
                samples.add(GATKVariantContextUtils.mergedSampleName(val.getKey(), sample,
                        mergeOption == GATKVariantContextUtils.GenotypeMergeType.UNIQUIFY));
            }
        }

        return samples;
    }

    /**
     * Given a collection of samples and a collection of regular expressions, generates the set of samples that match
     * each expression
     * @param originalSamples list of samples to select samples from
     * @param sampleExpressions list of expressions to use for matching samples
     * @return the set of samples from originalSamples that satisfy at least one of the expressions in sampleExpressions
     */
    public static Collection<String> matchSamplesExpressions (Collection<String> originalSamples, Collection<String> sampleExpressions) {
        // Now, check the expressions that weren't used in the previous step, and use them as if they're regular expressions
        final Set<String> samples = new HashSet<String>();
        if (sampleExpressions != null) {
            samples.addAll(ListFileUtils.includeMatching(originalSamples, sampleExpressions, false));
        }
        return samples;
    }

    /**
     * Given a list of files with sample names it reads all files and creates a list of unique samples from all these files.
     * @param files list of files with sample names in
     * @return a collection of unique samples from all files
     */
    public static Collection<String> getSamplesFromFiles (Collection<File> files) {
        final Set<String> samplesFromFiles = new HashSet<String>();
        if (files != null) {
            for (final File file : files) {
                try (XReadLines reader = new XReadLines(file)) {
                    List<String> lines = reader.readLines();
                    for (final String line : lines) {
                        samplesFromFiles.add(line);
                    }
                } catch (IOException e) {
                    throw new UserException.CouldNotReadInputFile(file, e);
                }
            }
        }
        return samplesFromFiles;
    }

    /**
     * Get IDs from a file
     *
     * @param file file containing the IDs
     * @return set of IDs or null if the file is null
     * @throws UserException.CouldNotReadInputFile if could not read the file
     */
    private Set<String> getIDsFromFile(final File file){
        /** load in the IDs file to a hashset for matching */
        if (file != null) {
            Set<String> ids = new HashSet<>();
            try (final XReadLines xrl = new XReadLines(file)) {
                for (final java.lang.String line : xrl.readLines()) {
                    ids.add(line.trim());
                }
                logger.info("Selecting only variants with one of " + ids.size() + " IDs from " + file);
            } catch (IOException e) {
                throw new UserException.CouldNotReadInputFile(file, e);
            }
            return ids;
        }

        return null;
    }

    // TODO: possiblyInvertFilterExpression and invertLogic were taken from Adam's VariantFiltration branch - should
    // these be lifted into VariantWalker ?
    /**
     * Prepend inverse phrase to description if --invertFilterExpression
     *
     * @param description the description
     * @return the description with inverse prepended if --invert_filter_expression
     */
    private String possiblyInvertFilterExpression(final String description){
        return invertSelect ? "Inverse of: " + description : description;
    }

    /**
     * Invert logic if specified
     *
     * @param logic boolean logical operation value
     * @param invert whether to invert logic
     * @return invert logic if invert flag is true, otherwise leave the logic
     */
    private static boolean invertLogic(final boolean logic, final boolean invert){
        return invert ? !logic : logic;
    }

    /*
     * Determines if any of the alternate alleles are greater than the max indel size or less than the min indel size
     *
     * @param vc            the variant context to check
     * @param maxIndelSize  the maximum size of allowed indels
     * @param minIndelSize  the minimum size of allowed indels
     * @return true if the VC contains an indel larger than maxIndelSize or less than the minIndelSize, false otherwise
     */
    protected static boolean containsIndelLargerOrSmallerThan(final VariantContext vc, final int maxIndelSize, final int minIndelSize) {
        final List<Integer> lengths = vc.getIndelLengths();
        if (lengths == null)
            return false;

        for (final Integer indelLength : lengths) {
            if (Math.abs(indelLength) > maxIndelSize || Math.abs(indelLength) < minIndelSize)
                return true;
        }

        return false;
    }

    /**
     * Find the number of filtered samples
     *
     * @param vc the variant rod VariantContext
     * @return number of filtered samples
     */
    private int numFilteredGenotypes(final VariantContext vc){
        if (vc == null)
            return 0;

        int numFiltered = 0;
        // check if we find it in the variant rod
        final GenotypesContext genotypes = vc.getGenotypes(samples);
        for (final Genotype g : genotypes)
            if (g.isFiltered() && !g.getFilters().isEmpty())
                numFiltered++;

        return numFiltered;
    }

    /**
     * Checks if vc has a variant call for (at least one of) the samples.
     *
     * @param vc the variant rod VariantContext. Here, the variant is the dataset you're looking for discordances to (e.g. HapMap)
     * @param compVCs the comparison VariantContext (discordance)
     * @return true VariantContexts are discordant, false otherwise
     */
    private boolean isDiscordant (final VariantContext vc, final Collection<VariantContext> compVCs) {
        if (vc == null)
            return false;

        // if we're not looking at specific samples then the absence of a compVC means discordance
        if (noSamplesSpecified)
            return (compVCs == null || compVCs.isEmpty());

        // check if we find it in the variant rod
        final GenotypesContext genotypes = vc.getGenotypes(samples);
        for (final Genotype g : genotypes) {
            if (sampleHasVariant(g)) {
                // There is a variant called (or filtered with not exclude filtered option set) that is not HomRef for at least one of the samples.
                if (compVCs == null)
                    return true;
                // Look for this sample in the all vcs of the comp ROD track.
                boolean foundVariant = false;
                for (final VariantContext compVC : compVCs) {
                    if (haveSameGenotypes(g, compVC.getGenotype(g.getSampleName()))) {
                        foundVariant = true;
                        break;
                    }
                }
                // if (at least one sample) was not found in all VCs of the comp ROD, we have discordance
                if (!foundVariant)
                    return true;
            }
        }
        return false; // we only get here if all samples have a variant in the comp rod.
    }

    /**
     * Checks if the two variants have the same genotypes for the selected samples
     *
     * @param vc the variant rod VariantContext.
     * @param compVCs the comparison VariantContext
     * @return true if VariantContexts are concordant, false otherwise
     */
    private boolean isConcordant (final VariantContext vc, final Collection<VariantContext> compVCs) {
        if (vc == null || compVCs == null || compVCs.isEmpty())
            return false;

        // if we're not looking for specific samples then the fact that we have both VCs is enough to call it concordant.
        if (noSamplesSpecified)
            return true;

        // make a list of all samples contained in this variant VC that are being tracked by the user command line arguments.
        final Set<String> variantSamples = vc.getSampleNames();
        variantSamples.retainAll(samples);

        // check if we can find all samples from the variant rod in the comp rod.
        for (final String sample : variantSamples) {
            boolean foundSample = false;
            for (final VariantContext compVC : compVCs) {
                final Genotype varG = vc.getGenotype(sample);
                final Genotype compG = compVC.getGenotype(sample);
                if (haveSameGenotypes(varG, compG)) {
                    foundSample = true;
                    break;
                }
            }
            // if at least one sample doesn't have the same genotype, we don't have concordance
            if (!foundSample) {
                return false;
            }
        }
        return true;
    }

    private boolean sampleHasVariant(final Genotype g) {
        return (g !=null && !g.isHomRef() && (g.isCalled() || (g.isFiltered() && !XLfiltered)));
    }

    private boolean haveSameGenotypes(final Genotype g1, final Genotype g2) {
        if (g1 == null || g2 == null)
            return false;

        if ((g1.isCalled() && g2.isFiltered()) ||
                (g2.isCalled() && g1.isFiltered()) ||
                (g1.isFiltered() && g2.isFiltered() && XLfiltered))
            return false;

        final List<Allele> a1s = g1.getAlleles();
        final List<Allele> a2s = g2.getAlleles();
        return (a1s.containsAll(a2s) && a2s.containsAll(a1s));
    }

    /**
     * Helper method to subset a VC record, modifying some metadata stored in the INFO field (i.e. AN, AC, AF).
     *
     * @param vc       the VariantContext record to subset
     * @param preserveAlleles should we trim constant sequence from the beginning and/or end of all alleles, or preserve it?
     * @param removeUnusedAlternates removes alternate alleles with AC=0
     * @return the subsetted VariantContext
     */

    private VariantContext subsetRecord(final VariantContext vc, final boolean preserveAlleles, final boolean removeUnusedAlternates) {
        //subContextFromSamples() always decodes the vc, which is a fairly expensive operation.  Avoid if possible
        if (noSamplesSpecified && !removeUnusedAlternates)
            return vc;

        // strip out the alternate alleles that aren't being used
        final VariantContext sub = vc.subContextFromSamples(samples, removeUnusedAlternates);

        // If no subsetting happened, exit now
        if (sub.getNSamples() == vc.getNSamples() && sub.getNAlleles() == vc.getNAlleles())
            return vc;

        final VariantContextBuilder builder = new VariantContextBuilder(sub);

        // if there are fewer alternate alleles now in the selected VC, we need to fix the PL and AD values
        GenotypesContext newGC = GATKVariantContextUtils.updatePLsAndAD(sub, vc);

        // since the VC has been subset (either by sample or allele), we need to strip out the MLE tags
        builder.rmAttribute(GATKVCFConstants.MLE_ALLELE_COUNT_KEY);
        builder.rmAttribute(GATKVCFConstants.MLE_ALLELE_FREQUENCY_KEY);

        // Remove a fraction of the genotypes if needed
        if (fractionGenotypes > 0) {
            final ArrayList<Genotype> genotypes = new ArrayList<>();
            for (final Genotype genotype : newGC) {
                //Set genotype to no call if it falls in the fraction.
                if (fractionGenotypes > 0 && randomGenotypes.nextDouble() < fractionGenotypes) {
                    genotypes.add(new GenotypeBuilder(genotype).alleles(diploidNoCallAlleles).noGQ().make());
                }
                else {
                    genotypes.add(genotype);
                }
            }
            newGC = GenotypesContext.create(genotypes);
        }

        builder.genotypes(newGC);

        addAnnotations(builder, vc, sub.getSampleNames());

        final VariantContext subset = builder.make();

        final VariantContext trimmed = preserveAlleles? subset : GATKVariantContextUtils.trimAlleles(subset,true,true);

        return trimmed;
    }

    /**
     * If --setFilteredGtToNocall, set filtered genotypes to no-call
     *
     * @param vc the VariantContext record to set filtered genotypes to no-call
     * @param filteredGenotypesToNocall  set filtered genotypes to non-call?
     * @return the VariantContext with no-call genotypes if the sample was filtered
     */
    private VariantContext setFilteredGenotypeToNocall(final VariantContext vc, final boolean filteredGenotypesToNocall) {

        if (!filteredGenotypesToNocall)
            return vc;

        final VariantContextBuilder builder = new VariantContextBuilder(vc);
        final GenotypesContext genotypes = GenotypesContext.create(vc.getGenotypes().size());

        for (final Genotype g : vc.getGenotypes()) {
            if (g.isCalled() && g.isFiltered())
                genotypes.add(new GenotypeBuilder(g).alleles(diploidNoCallAlleles).make());
            else
                genotypes.add(g);
        }

        return builder.genotypes(genotypes).make();
    }
    /*
     * Add annotations to the new VC
     *
     * @param builder     the new VC to annotate
     * @param originalVC  the original VC
     * @param selectedSampleNames the post-selection list of sample names
     */
    private void addAnnotations (final VariantContextBuilder builder, final VariantContext originalVC, final Set<String> selectedSampleNames) {
        // @TODO: this TODO was in GATK - is this still the case ?
        if (fullyDecode) return; // TODO -- annotations are broken with fully decoded data

        if (keepOriginalChrCounts) {
            final int[] indexOfOriginalAlleleForNewAllele;
            final List<Allele> newAlleles = builder.getAlleles();
            final int numOriginalAlleles = originalVC.getNAlleles();

            // if the alleles already match up, we can just copy the previous list of counts
            if (numOriginalAlleles == newAlleles.size()) {
                indexOfOriginalAlleleForNewAllele = null;
            }
            // otherwise we need to parse them and select out the correct ones
            else {
                indexOfOriginalAlleleForNewAllele = new int[newAlleles.size() - 1];
                Arrays.fill(indexOfOriginalAlleleForNewAllele, -1);

                // note that we don't care about the reference allele at position 0
                for (int newI = 1; newI < newAlleles.size(); newI++) {
                    final Allele newAlt = newAlleles.get(newI);
                    for (int oldI = 0; oldI < numOriginalAlleles - 1; oldI++) {
                        if (newAlt.equals(originalVC.getAlternateAllele(oldI), false)) {
                            indexOfOriginalAlleleForNewAllele[newI - 1] = oldI;
                            break;
                        }
                    }
                }
            }

            if (originalVC.hasAttribute(VCFConstants.ALLELE_COUNT_KEY))
                builder.attribute(GATKVCFConstants.ORIGINAL_AC_KEY,
                        getReorderedAttributes(originalVC.getAttribute(VCFConstants.ALLELE_COUNT_KEY), indexOfOriginalAlleleForNewAllele));
            if (originalVC.hasAttribute(VCFConstants.ALLELE_FREQUENCY_KEY))
                builder.attribute(GATKVCFConstants.ORIGINAL_AF_KEY,
                        getReorderedAttributes(originalVC.getAttribute(VCFConstants.ALLELE_FREQUENCY_KEY), indexOfOriginalAlleleForNewAllele));
            if (originalVC.hasAttribute(VCFConstants.ALLELE_NUMBER_KEY))
                builder.attribute(GATKVCFConstants.ORIGINAL_AN_KEY, originalVC.getAttribute(VCFConstants.ALLELE_NUMBER_KEY));
        }

        VariantContextUtils.calculateChromosomeCounts(builder, false);

        if (keepOriginalDepth && originalVC.hasAttribute(VCFConstants.DEPTH_KEY))
            builder.attribute(GATKVCFConstants.ORIGINAL_DP_KEY, originalVC.getAttribute(VCFConstants.DEPTH_KEY));

        boolean sawDP = false;
        int depth = 0;
        for (final String sample : selectedSampleNames ) {
            final Genotype g = originalVC.getGenotype(sample);

            if (!g.isFiltered()) {
                if (g.hasDP()) {
                    depth += g.getDP();
                    sawDP = true;
                }
            }
        }

        if (sawDP) {
            builder.attribute(VCFConstants.DEPTH_KEY, depth);
        }
    }

    /**
     * Pulls out the appropriate tokens from the old ordering of an attribute to the new ordering
     *
     * @param attribute               the non-null attribute (from the INFO field)
     * @param oldToNewIndexOrdering   the mapping from new to old ordering
     * @return non-null Object attribute
     */
    private Object getReorderedAttributes(final Object attribute, final int[] oldToNewIndexOrdering) {
        // if the ordering is the same, then just use the original attribute
        if (oldToNewIndexOrdering == null) {
            return attribute;
        }

        // break the original attributes into separate tokens; unfortunately, this means being smart about class types
        final Object[] tokens;
        if (attribute.getClass().isArray()) {
            tokens = (Object[]) attribute;
        }
        else if (List.class.isAssignableFrom(attribute.getClass())) {
            tokens = ((List) attribute).toArray();
        }
        else {
            tokens = attribute.toString().split(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR);
        }

        final List<Object> result = new ArrayList<>();
        for (final int index : oldToNewIndexOrdering ) {
            if (index >= tokens.length) {
                throw new IllegalArgumentException("the old attribute has an incorrect number of elements: " + attribute);
            }
            result.add(tokens[index]);
        }
        return result;
    }

    /**
     * Need the number of filtered genotypes samples?
     *
     * @return true if any of the filtered genotype samples arguments is used (not the default value), false otherwise
     */
    private boolean needNumFilteredGenotypes(){
        return maxFilteredGenotypes != MAX_FILTERED_GENOTYPES_DEFAULT_VALUE ||
                minFilteredGenotypes != MIN_FILTERED_GENOTYPES_DEFAULT_VALUE ||
                maxFractionFilteredGenotypes != MAX_FRACTION_FILTERED_GENOTYPES_DEFAULT_VALUE ||
                minFractionFilteredGenotypes != MIN_FRACTION_FILTERED_GENOTYPES_DEFAULT_VALUE;
    }
}
