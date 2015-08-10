package org.broadinstitute.hellbender.engine;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.reference.ReferenceSequence;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.fasta.CachingIndexedFastaSequenceFile;
import org.broadinstitute.hellbender.utils.iterators.ByteArrayIterator;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * Manages traversals and queries over reference data.
 *
 * Supports targeted queries over the reference by interval and over the entire reference.
 */
public interface ReferenceDataSource extends GATKDataSource<Byte>, AutoCloseable {

    /**
     * Start an iteration over the entire reference. Not yet supported!
     *
     * See the BaseUtils class for guidance on how to work with bases in this format.
     *
     * @return iterator over all bases in this reference
     */
    @Override
    default public Iterator<Byte> iterator() {
        throw new UnsupportedOperationException("Iteration over entire reference not yet implemented");
    }


    /**
     * Query a specific interval on this reference, and get back an iterator over the bases spanning that interval.
     *
     * See the BaseUtils class for guidance on how to work with bases in this format.
     *
     * The default implemention uses {@link ByteArrayIterator}.
     *
     * @param interval query interval
     * @return iterator over the bases spanning the query interval
     */
    @Override
    default Iterator<Byte> query( final SimpleInterval interval ) {
        // TODO: need a way to iterate lazily over reference bases without necessarily loading them all into memory at once
        return new ByteArrayIterator(queryAndPrefetch(interval).getBases());
    }

    /**
     * Query a specific interval on this reference, and get back all bases spanning that interval at once.
     * Call getBases() on the returned ReferenceSequence to get the actual reference bases. See the BaseUtils
     * class for guidance on how to work with bases in this format.
     *
     * The default implementation calls #queryAndPrefetch(contig, start, stop).
     *
     * @param interval query interval
     * @return a ReferenceSequence containing all bases spanning the query interval, prefetched
     */
    default public ReferenceSequence queryAndPrefetch( final SimpleInterval interval ) {
        return queryAndPrefetch(interval.getContig(), interval.getStart(), interval.getEnd());
    }

    /**
     * Query a specific interval on this reference, and get back all bases spanning that interval at once.
     * Call getBases() on the returned ReferenceSequence to get the actual reference bases. See the BaseUtils
     * class for guidance on how to work with bases in this format.
     *
     * @param contig query interval contig
     * @param start query interval start
     * @param stop query interval stop
     * @return a ReferenceSequence containing all bases spanning the query interval, prefetched
     */
    public ReferenceSequence queryAndPrefetch( final String contig, final long start , final long stop);

    /**
     * Get the sequence dictionary for this reference
     *
     * @return SAMSequenceDictionary for this reference
     */
    public SAMSequenceDictionary getSequenceDictionary();

    /**
     * Permanently close this data source. The default implementation does nothing.
     */
    @Override
    default public void close(){
        //do nothing
    }
}
