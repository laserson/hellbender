package org.broadinstitute.hellbender.utils.variant;

import htsjdk.variant.vcf.*;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains the VCFHeaderLine definitions for the annotation keys in GATKVCFConstants.
 * VCF-standard header lines are in VCFStandardHeaderLines, in htsjdk
 */
public class GATKVCFHeaderLines {

    public static VCFInfoHeaderLine getInfoLine(final String id) { return infoLines.get(id); }
    // public static VCFFormatHeaderLine getFormatLine(final String id) { return formatLines.get(id); }
    // public static VCFFilterHeaderLine getFilterLine(final String id) { return filterLines.get(id); }

    private static Map<String, VCFInfoHeaderLine> infoLines = new HashMap<>(60);
    // private static Map<String, VCFFormatHeaderLine> formatLines = new HashMap<>(25);
    // private static Map<String, VCFFilterHeaderLine> filterLines = new HashMap<>(2);

    // private static void addFormatLine(final VCFFormatHeaderLine line) {
    //     formatLines.put(line.getID(), line);
    // }

    private static void addInfoLine(final VCFInfoHeaderLine line) {
        infoLines.put(line.getID(), line);
    }

    // private static void addFilterLine(final VCFFilterHeaderLine line) {
    //     filterLines.put(line.getID(), line);
    // }

    static {
        addInfoLine(new VCFInfoHeaderLine(GATKVCFConstants.MLE_ALLELE_COUNT_KEY, VCFHeaderLineCount.A, VCFHeaderLineType.Integer, "Maximum likelihood expectation (MLE) for the allele counts (not necessarily the same as the AC), for each ALT allele, in the same order as listed"));
        addInfoLine(new VCFInfoHeaderLine(GATKVCFConstants.MLE_ALLELE_FREQUENCY_KEY, VCFHeaderLineCount.A, VCFHeaderLineType.Float, "Maximum likelihood expectation (MLE) for the allele frequency (not necessarily the same as the AF), for each ALT allele, in the same order as listed"));

        addInfoLine(new VCFInfoHeaderLine(GATKVCFConstants.ORIGINAL_AC_KEY, VCFHeaderLineCount.A, VCFHeaderLineType.Integer, "Original AC"));
        addInfoLine(new VCFInfoHeaderLine(GATKVCFConstants.ORIGINAL_AF_KEY, VCFHeaderLineCount.A, VCFHeaderLineType.Float, "Original AF"));
        addInfoLine(new VCFInfoHeaderLine(GATKVCFConstants.ORIGINAL_AN_KEY, 1, VCFHeaderLineType.Integer, "Original AN"));
        addInfoLine(new VCFInfoHeaderLine(GATKVCFConstants.ORIGINAL_DP_KEY, 1, VCFHeaderLineType.Integer, "Original DP"));
    }
}
