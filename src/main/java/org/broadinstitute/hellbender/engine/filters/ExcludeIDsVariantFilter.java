package org.broadinstitute.hellbender.engine.filters;

import htsjdk.variant.variantcontext.VariantContext;
import org.broadinstitute.hellbender.utils.Utils;

import java.util.Set;
import java.util.HashSet;

/**
 * Exclude variants with any of these IDs.
 * Matching is done by case-sensitive exact match.
 */
public final class ExcludeIDsVariantFilter implements VariantFilter {
    private static final long serialVersionUID = 1L;

    private final HashSet<String> excludeIDs = new HashSet<>();

    public ExcludeIDsVariantFilter(Set<String> discardIDs) {
        Utils.nonNull(discardIDs);
        excludeIDs.addAll(discardIDs);
    }

    @Override
    public boolean test(final VariantContext vc) {
        return !excludeIDs.contains(vc.getID());
    }
}
