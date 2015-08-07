package org.broadinstitute.hellbender.engine;

import htsjdk.samtools.SAMReadGroupRecord;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.GenomeLoc;
import org.broadinstitute.hellbender.utils.pileup.PileupElement;
import org.broadinstitute.hellbender.utils.pileup.ReadPileup;

import java.util.*;

/**
 * Useful utilities for storing different AlignmentContexts
 */
public class AlignmentContextUtils {

    // Definitions:
    //   COMPLETE = full alignment context
    //   FORWARD  = reads on forward strand
    //   REVERSE  = reads on forward strand
    //
    public enum ReadOrientation { COMPLETE, FORWARD, REVERSE }

    private AlignmentContextUtils() {
        // cannot be instantiated
    }

    /**
     * Returns a potentially derived subcontext containing only forward, reverse, or in fact all reads
     * in alignment context context.
     *
     * @param context
     * @param type
     * @return
     */
    public static AlignmentContext stratify(AlignmentContext context, ReadOrientation type) {
        switch(type) {
            case COMPLETE:
                return context;
            case FORWARD:
                return new AlignmentContext(context.getLocation(),context.getBasePileup().makeFilteredPileup(pe -> !pe.getRead().isReverseStrand()));
            case REVERSE:
                return new AlignmentContext(context.getLocation(),context.getBasePileup().makeFilteredPileup(pe -> pe.getRead().isReverseStrand()));
            default:
                throw new GATKException("Unable to get alignment context for type = " + type);
        }
    }

    public static Map<String, AlignmentContext> splitContextBySampleName(AlignmentContext context) {
        return splitContextBySampleName(context, null);
    }

    /**
     * Splits the given AlignmentContext into a StratifiedAlignmentContext per sample, but referencd by sample name instead
     * of sample object.
     *
     * @param context                the original pileup
     *
     * @return a Map of sample name to StratifiedAlignmentContext
     *
     **/
    public static Map<String, AlignmentContext> splitContextBySampleName(AlignmentContext context, String assumedSingleSample) {
        GenomeLoc loc = context.getLocation();
        HashMap<String, AlignmentContext> contexts = new HashMap<String, AlignmentContext>();

        for(String sample: context.getBasePileup().getSamples()) {
            ReadPileup pileupBySample = context.getBasePileup().getPileupForSample(sample);

            // Don't add empty pileups to the split context.
            if(pileupBySample.isEmpty())
                continue;

            if(sample != null)
                contexts.put(sample, new AlignmentContext(loc, pileupBySample));
            else {
                if(assumedSingleSample == null) {
                    throw new UserException.ReadMissingReadGroup(pileupBySample.iterator().next().getRead());
                }
                contexts.put(assumedSingleSample,new AlignmentContext(loc, pileupBySample));
            }
        }

        return contexts;
    }

    /**
     * Splits the AlignmentContext into one context per read group
     *
     * @param context the original pileup
     * @return a Map of ReadGroup to AlignmentContext, or an empty map if context has no base pileup
     *
     **/
    public static Map<SAMReadGroupRecord, AlignmentContext> splitContextByReadGroup(AlignmentContext context, Collection<SAMReadGroupRecord> readGroups) {
        HashMap<SAMReadGroupRecord, AlignmentContext> contexts = new HashMap<SAMReadGroupRecord, AlignmentContext>();

        for (SAMReadGroupRecord rg : readGroups) {
            ReadPileup rgPileup = context.getBasePileup().getPileupForReadGroup(rg.getReadGroupId());
            if ( rgPileup != null ) // there we some reads for RG
                contexts.put(rg, new AlignmentContext(context.getLocation(), rgPileup));
        }

        return contexts;
    }

    public static Map<String, AlignmentContext> splitContextBySampleName(ReadPileup pileup) {
        return splitContextBySampleName(new AlignmentContext(pileup.getLocation(), pileup));
    }


    public static AlignmentContext joinContexts(Collection<AlignmentContext> contexts) {
        // validation
        GenomeLoc loc = contexts.iterator().next().getLocation();
        for(AlignmentContext context: contexts) {
            if(!loc.equals(context.getLocation()))
                throw new GATKException("Illegal attempt to join contexts from different genomic locations");
        }

        List<PileupElement> pe = new ArrayList<>();
        for(AlignmentContext context: contexts) {
            for(PileupElement pileupElement: context.basePileup)
                pe.add(pileupElement);
        }
        return new AlignmentContext(loc, new ReadPileup(loc,pe));
    }
}