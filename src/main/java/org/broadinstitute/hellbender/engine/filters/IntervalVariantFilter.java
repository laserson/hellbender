package org.broadinstitute.hellbender.engine.filters;

import htsjdk.samtools.util.Locatable;
import htsjdk.variant.variantcontext.VariantContext;
import org.broadinstitute.hellbender.utils.GenomeLoc;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.variant.GATKVariantContextUtils;

import java.util.List;

/**
 * Keep only variants that overlap any of the supplied genomic intervals.
 */
public final class IntervalVariantFilter implements VariantFilter {
    private static final long serialVersionUID = 1L;

    private final List<GenomeLoc> genomeLocs;

    public IntervalVariantFilter(List<GenomeLoc> intervals) {
        Utils.nonNull(intervals);
        genomeLocs = intervals;
    }

    @Override
    public boolean test( final VariantContext vc ) {
        final Locatable targetLoc = GATKVariantContextUtils.getLocation(vc);
        return genomeLocs.stream().anyMatch(loc -> IntervalUtils.overlaps(loc, targetLoc));
    }
}
