package org.broadinstitute.hellbender.tools.dataflow.transforms;

import com.google.api.client.json.GenericJson;
import com.google.api.client.util.Key;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SamPairUtil;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@DefaultCoder(GenericJsonCoder.class)
public final class InsertSizeAggregationLevel extends GenericJson implements Serializable{
    public final static long serialVersionUID = 1l;
    private static final String PU = "PU";

    @Key
    private String orientation;
    @Key
    private String readGroup;
    @Key
    private String library;
    @Key
    private String sample;

    public SamPairUtil.PairOrientation getOrientation() {
        return SamPairUtil.PairOrientation.valueOf(orientation);
    }

    public String getReadGroup() {
        return readGroup;
    }

    public String getLibrary() {
        return library;
    }

    public String getSample() {
        return sample;
    }


    public InsertSizeAggregationLevel(final GATKRead read, final SAMFileHeader header, final boolean includeLibrary, final boolean includeReadGroup, final boolean includeSample) {
        this.orientation = SamPairUtil.getPairOrientation(read.convertToSAMRecord(header)).toString();
        this.library = includeLibrary ? ReadUtils.getLibrary(read, header) : null;
        this.readGroup = includeReadGroup ? ReadUtils.getSAMReadGroupRecord(read, header).getAttribute(PU): null;
        this.sample = includeSample ? ReadUtils.getSampleName(read, header) : null;
    }

    /**
     * This is required by {@link GenericJsonCoder} but should not be used
     */
    public InsertSizeAggregationLevel() {
    }

    ;

    public InsertSizeAggregationLevel(final String orientation, final String library, final String readgroup, final String sample) {
        this.orientation = orientation;
        this.library = library;
        this.readGroup = readgroup;
        this.sample = sample;
    }

    public static List<InsertSizeAggregationLevel> getKeysForAllAggregationLevels(final GATKRead read, final SAMFileHeader header, final boolean includeAll, final boolean includeLibrary, final boolean includeReadGroup, final boolean includeSample) {
        final List<InsertSizeAggregationLevel> aggregationLevels = new ArrayList<>();
        if (includeAll) {
            aggregationLevels.add(new InsertSizeAggregationLevel(read, header, false, false, false));
        }
        if (includeLibrary) {
            aggregationLevels.add(new InsertSizeAggregationLevel(read, header, true, false, true));
        }
        if (includeReadGroup) {
            aggregationLevels.add(new InsertSizeAggregationLevel(read, header, true, true, true));
        }
        if (includeSample) {
            aggregationLevels.add(new InsertSizeAggregationLevel(read, header, false, false, true));
        }
        return aggregationLevels;

    }


    @Override
    public String toString() {
        return "AggregationLevel{" +
                "orientation=" + orientation +
                ", readGroup='" + readGroup + '\'' +
                ", library='" + library + '\'' +
                ", sample='" + sample + '\'' +
                '}';
    }

    public String getValueLabel(){
        String prefix;
        if (readGroup != null) {
            prefix = readGroup;
        }
        else if (this.library != null) {
            prefix = library;
        }
        else if (this.sample != null) {
            prefix = sample;
        }
        else {
            prefix = "All_Reads";
        }

        return prefix + "." + orientation.toLowerCase() + "_count";
    }


}
