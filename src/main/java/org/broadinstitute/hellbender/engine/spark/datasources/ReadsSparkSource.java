package org.broadinstitute.hellbender.engine.spark.datasources;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.genomics.utils.ReadUtils;
import com.google.common.collect.Lists;
import htsjdk.samtools.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.ReadsDataSource;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.GoogleGenomicsReadToGATKReadAdapter;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import java.io.File;
import java.util.List;

public class ReadsSparkSource {
    private final JavaSparkContext ctx;
    public ReadsSparkSource(JavaSparkContext ctx) {
        this.ctx = ctx;
    }

    public JavaRDD<GATKRead> getSerialReads(String bam) {
        final SAMFileHeader readsHeader = getHeader(bam);
        List<SimpleInterval> intervals = IntervalUtils.getAllIntervalsForReference(readsHeader.getSequenceDictionary());
        final SamReaderFactory samReaderFactory = SamReaderFactory.makeDefault().validationStringency(ValidationStringency.SILENT);

        ReadsDataSource bam2 = new ReadsDataSource(new File(bam), samReaderFactory);
        bam2.setIntervalsForTraversal(intervals);
        List<GATKRead> records = Lists.newArrayList();
        for ( GATKRead read : bam2 ) {
            records.add(read);
        }
        return ctx.parallelize(records);
    }

    public JavaRDD<GATKRead> getParallelReads(String bam, List<SimpleInterval> intervals) {
        JavaPairRDD<LongWritable, SAMRecordWritable> rdd2 = ctx.newAPIHadoopFile(
                bam, AnySAMInputFormat.class, LongWritable.class, SAMRecordWritable.class,
                new Configuration());

        return rdd2.map(v1 -> {
            SAMRecord sam = v1._2().get();
            if (samRecordOverlaps(sam, intervals)) {
                try {
                    Read read = ReadUtils.makeRead(sam);
                    if (read == null) {
                        throw new GATKException("null read");
                    }
                    return (GATKRead) (new GoogleGenomicsReadToGATKReadAdapter(read));
                } catch (SAMException e) {
                    // do nothing if silent
                }
            }
            return null;

        }).filter(v1 -> v1 != null);
    }

    public JavaRDD<GATKRead> getParallelReads(String bam) {
        final SAMFileHeader readsHeader = getHeader(bam);
        List<SimpleInterval> intervals = IntervalUtils.getAllIntervalsForReference(readsHeader.getSequenceDictionary());
        return getParallelReads(bam, intervals);
    }

    public SAMFileHeader getHeader(String bam) {
        return SamReaderFactory.makeDefault().getFileHeader(new File(bam));
    }

    /**
     * Tests if a given SAMRecord overlaps any interval in a collection.
     */
    //TODO: remove this method when https://github.com/broadinstitute/hellbender/issues/559 is fixed
    private static boolean samRecordOverlaps( SAMRecord record, List<SimpleInterval> intervals ) {
        if (intervals == null || intervals.isEmpty()) {
            return true;
        }
        for (SimpleInterval interval : intervals) {
            if (interval.overlaps(record)) {
                return true;
            }
        }
        return false;
    }
}