package org.broadinstitute.hellbender.engine.spark;

import com.beust.jcommander.internal.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.dataflow.datasources.VariantShard;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.engine.spark.datasources.VariantsSparkSource;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.variant.Variant;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public class JoinReadsWithVariants {

    public static JavaPairRDD<GATKRead, Iterable<Variant>> Join(
            JavaRDD<GATKRead> reads, JavaRDD<Variant> variants) {

        JavaPairRDD<VariantShard, GATKRead> readsWShards = reads.flatMapToPair(gatkRead -> {
            List<VariantShard> shards = VariantShard.getVariantShardsFromInterval(gatkRead);
            List<Tuple2<VariantShard, GATKRead>> out = Lists.newArrayList();
            for (VariantShard shard : shards) {
                out.add(new Tuple2<>(shard, gatkRead));
            }
            return out;
        });

        JavaPairRDD<VariantShard, Variant> variantsWShards = variants.flatMapToPair(variant -> {
            List<VariantShard> shards = VariantShard.getVariantShardsFromInterval(variant);
            List<Tuple2<VariantShard, Variant>> out = Lists.newArrayList();
            for (VariantShard shard : shards) {
                out.add(new Tuple2<>(shard, variant));
            }
            return out;
        });

        JavaPairRDD<VariantShard, Tuple2<Iterable<GATKRead>, Iterable<Variant>>> cogroup = readsWShards.cogroup(variantsWShards);

        JavaPairRDD<GATKRead, Variant> allPairs = cogroup.flatMapToPair(cogroupValue -> {
            Iterable<GATKRead> iReads = cogroupValue._2()._1();
            Iterable<Variant> iVariants = cogroupValue._2()._2();

            List<Tuple2<GATKRead, Variant>> out = Lists.newArrayList();
            // For every read, find every overlapping variant.
            for (GATKRead r : iReads) {
                boolean foundVariants = false;
                SimpleInterval interval = new SimpleInterval(r);
                for (Variant v : iVariants) {
                    if (interval.overlaps(v)) {
                        foundVariants = true;
                        out.add(new Tuple2<>(r, v));
                    }
                }
                if (!foundVariants) {
                    out.add(new Tuple2<>(r, null));
                }
                // TODO: Check if any variants are found, if not output read with no variants.
            }
            return out;
        });
        return allPairs.distinct().groupByKey();
    }
}