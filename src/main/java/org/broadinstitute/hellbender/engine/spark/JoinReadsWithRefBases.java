package org.broadinstitute.hellbender.engine.spark;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.dev.tools.walkers.bqsr.BaseRecalibratorDataflow;
import org.broadinstitute.hellbender.engine.dataflow.datasources.RefAPIMetadata;
import org.broadinstitute.hellbender.engine.dataflow.datasources.RefAPISource;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReferenceShard;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import scala.Tuple2;

import java.util.List;
import java.util.Map;


public class JoinReadsWithRefBases {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: JoinReadsWithRefBases <apiKey>");
            System.exit(1);
        }
        String bam = "src/test/resources/org/broadinstitute/hellbender/tools/BQSR/HiSeq.1mb.1RG.2k_lines.alternate.bam";

        SparkConf sparkConf = new SparkConf().setAppName("JoinReadsWithRefBases")
                .setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "org.broadinstitute.hellbender.engine.spark.GATKRegistrator");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        ReadsSparkSource readSource = new ReadsSparkSource(ctx);
        JavaRDD<GATKRead> reads = readSource.getParallelReads(bam);

        String referenceName = "EOSt9JOVhp3jkwE";
        Map<String, String> referenceNameToIdTable = Maps.newHashMap();
        referenceNameToIdTable.put("chr1", "EIaSo62VtfXT4AE");

        RefAPIMetadata refAPIMetadata = new RefAPIMetadata(referenceName, referenceNameToIdTable, BaseRecalibratorDataflow.BQSR_REFERENCE_WINDOW_FUNCTION, args[0]);


        JavaPairRDD<GATKRead, ReferenceBases> pair = Pair(refAPIMetadata, reads);
        Map<GATKRead, ReferenceBases> readRefBases = pair.collectAsMap();
        for (Map.Entry<GATKRead, ReferenceBases> next : readRefBases.entrySet()) {
            System.out.println(new String(next.getValue().getBases()));
        }
        ctx.stop();
    }

    public static JavaPairRDD<GATKRead, ReferenceBases> Pair(RefAPIMetadata refAPIMetadata,
                                                             JavaRDD<GATKRead> reads) {

        JavaPairRDD<ReferenceShard, GATKRead> shardRead = reads.mapToPair(gatkRead -> {
            ReferenceShard shard = ReferenceShard.getShardNumberFromInterval(gatkRead);
            return new Tuple2<>(shard, gatkRead);
        });

        JavaPairRDD<ReferenceShard, Iterable<GATKRead>> shardiRead = shardRead.groupByKey();

        return shardiRead.flatMapToPair(in -> {
            List<Tuple2<GATKRead, ReferenceBases>> out = Lists.newArrayList();
            Iterable<GATKRead> reads1 = in._2();
            SimpleInterval interval = SimpleInterval.getSpanningInterval(reads1);
            RefAPISource refAPISource = RefAPISource.getRefAPISource();
            ReferenceBases bases = refAPISource.getReferenceBases(refAPIMetadata, interval);
            for (GATKRead r : reads1) {
                final ReferenceBases subset = bases.getSubset(new SimpleInterval(r));
                out.add(new Tuple2<>(r, subset));
            }
            return out;
        });
    }
}