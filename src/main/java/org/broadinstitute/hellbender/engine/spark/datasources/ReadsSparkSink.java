package org.broadinstitute.hellbender.engine.spark.datasources;

import htsjdk.samtools.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.seqdoop.hadoop_bam.KeyIgnoringBAMOutputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import scala.Tuple2;

import java.io.IOException;

public class ReadsSparkSink {

    // Mostly as lifted from Hadoop-BAM.
    public static class MyOutputFormat extends KeyIgnoringBAMOutputFormat<NullWritable> {
        public static SAMFileHeader bamHeader = null;

        public static void setHeader(SAMFileHeader header) {
            bamHeader = header;
        }

        @Override
        public RecordWriter<NullWritable, SAMRecordWritable> getRecordWriter(TaskAttemptContext ctx) throws IOException {
            setSAMHeader(bamHeader);
            return super.getRecordWriter(ctx);
        }
    }

    public static void writeParallelReads(String bam, JavaRDD<GATKRead> rddReads, SAMFileHeader header) {
        MyOutputFormat.setHeader(header);

        // TODO: Sort reads first.
        JavaPairRDD<Text, SAMRecordWritable> rddSamRecordWriteable = rddReads.mapToPair(gatkRead -> {
            SAMRecord samRecord = gatkRead.convertToSAMRecord(header);
            SAMRecordWritable samRecordWritable = new SAMRecordWritable();
            samRecordWritable.set(samRecord);
            return new Tuple2<>(new Text(""), samRecordWritable);
        });
        rddSamRecordWriteable.saveAsNewAPIHadoopFile(bam, Text.class, SAMRecordWritable.class, MyOutputFormat.class);
    }

}