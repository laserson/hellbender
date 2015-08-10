package org.broadinstitute.hellbender.engine.spark;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.dev.tools.walkers.bqsr.BaseRecalibratorDataflow;
import org.broadinstitute.hellbender.engine.dataflow.datasources.ReadContextData;
import org.broadinstitute.hellbender.engine.dataflow.datasources.RefAPIMetadata;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.engine.spark.datasources.VariantsSparkSource;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import org.broadinstitute.hellbender.utils.variant.Variant;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public class AddContextDataToReadSpark {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: JoinReadsWithRefBases <apiKey>");
            System.exit(1);
        }
        String bam = "src/test/resources/org/broadinstitute/hellbender/tools/BQSR/HiSeq.1mb.1RG.2k_lines.alternate.bam";
        String vcf = "src/test/resources/org/broadinstitute/hellbender/tools/BQSR/dbsnp_132.b37.excluding_sites_after_129.chr17_69k_70k.vcf";

        SparkConf sparkConf = new SparkConf().setAppName("JoinReadsWithRefBases")
                .setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "org.broadinstitute.hellbender.engine.spark.GATKRegistrator");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        ReadsSparkSource readSouce = new ReadsSparkSource(ctx);
        JavaRDD<GATKRead> reads = readSouce.getParallelReads(bam);

        String referenceName = "EOSt9JOVhp3jkwE";
        Map<String, String> referenceNameToIdTable = Maps.newHashMap();
        referenceNameToIdTable.put("chr1", "EIaSo62VtfXT4AE");

        RefAPIMetadata refAPIMetadata = new RefAPIMetadata(referenceName, referenceNameToIdTable, BaseRecalibratorDataflow.BQSR_REFERENCE_WINDOW_FUNCTION, args[0]);

        VariantsSparkSource variantsSparkSource = new VariantsSparkSource(ctx);
        JavaRDD<Variant> variants = variantsSparkSource.getParallelVariants(vcf);
        JavaPairRDD<GATKRead, ReadContextData> readContextData = JoinContextData(reads, refAPIMetadata, variants);
        Map<GATKRead, ReadContextData> out = readContextData.collectAsMap();
        for (Map.Entry<GATKRead, ReadContextData> next : out.entrySet()) {
            System.out.println(next.getValue());
        }
        ctx.stop();
    }

    public static JavaPairRDD<GATKRead, ReadContextData> JoinContextData(
            JavaRDD<GATKRead> reads, RefAPIMetadata refAPIMetadata, JavaRDD<Variant> variants) {
        // Join Reads and Variants, Reads and ReferenceBases
        JavaPairRDD<GATKRead, Iterable<Variant>> readiVariants = JoinReadsWithVariants.Join(reads, variants);
        JavaPairRDD<GATKRead, ReferenceBases> readRefBases = JoinReadsWithRefBases.Pair(refAPIMetadata, reads);

        boolean assertsEnabled = false;
        assert assertsEnabled = true; // Intentional side-effect!!!
        // Now assertsEnabled is set to the correct value
        if (assertsEnabled) {
            assertSameReads(reads, readRefBases, readiVariants);
        }

        JavaPairRDD<GATKRead, Tuple2<Iterable<Iterable<Variant>>, Iterable<ReferenceBases>>> cogroup = readiVariants.cogroup(readRefBases);
        return cogroup.mapToPair(in -> {
            List<Iterable<Variant>> liVariants = Lists.newArrayList(in._2()._1());
            List<Variant> lVariants = Lists.newArrayList();
            if (!liVariants.isEmpty()) {
                final Iterable<Variant> iVariant = Iterables.getOnlyElement(in._2()._1());
                // It's possible for the iVariant to contain only a null variant, we don't
                // want to add that to the ReadContextData.
                final Variant next = iVariant.iterator().next();
                if (next != null) {
                    lVariants = Lists.newArrayList(iVariant);
                }
            }

            ReferenceBases refBases = Iterables.getOnlyElement(in._2()._2());
            ReadContextData readContextData = new ReadContextData(refBases, lVariants);
            return new Tuple2<>(in._1(), readContextData);
        });
    }

    private static void assertSameReads(JavaRDD<GATKRead> reads,
                                        JavaPairRDD<GATKRead, ReferenceBases> readRefBases,
                                        JavaPairRDD<GATKRead, Iterable<Variant>> readiVariants) {
        List<GATKRead> vReads = reads.collect();
        Map<GATKRead, ReferenceBases> vReadRef = readRefBases.collectAsMap();
        Map<GATKRead, Iterable<Variant>> vReadVariant = readiVariants.collectAsMap();

        // This assumes all rdds doesn't have any duplicates.
        JavaRDD<GATKRead> refBasesReads = readRefBases.keys();
        JavaRDD<GATKRead> variantsReads = readiVariants.keys();
        JavaRDD<GATKRead> distinctReads = reads.intersection(refBasesReads).intersection(variantsReads);

        long counts = reads.count();
        assert counts == distinctReads.count();
        assert counts == refBasesReads.count();
        assert counts == variantsReads.count();

        assert vReadRef.size() == vReads.size();
        assert vReadVariant.size() == vReads.size();
    }

}

