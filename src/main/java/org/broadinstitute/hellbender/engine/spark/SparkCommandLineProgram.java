package org.broadinstitute.hellbender.engine.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgram;

import java.io.File;
import java.io.Serializable;


public abstract class SparkCommandLineProgram extends CommandLineProgram implements Serializable {
    private static final long serialVersionUID = 1l;

    @Argument(fullName="project", doc="dataflow project id", optional=true)
    private String projectID;

    @Argument(fullName = "staging", doc="dataflow staging location, this should be a google bucket of the form gs://", optional = true)
    protected String stagingLocation;

    @Argument(doc = "path to the client secrets file for google cloud authentication",
            shortName = "secret", fullName = "client_secret", optional=true, mutex={"apiKey"})
    protected File clientSecret;

    @Argument(doc = "API Key for google cloud authentication",
            shortName = "apiKey", fullName = "apiKey", optional=true, mutex={"client_secret"})
    protected String apiKey = null;

    @Argument(doc = "Number of Spark workers to use.",
            shortName = "numWorkers", fullName = "numWorkers", optional=false)
    protected int numWorkers = 2;

    @Argument(fullName = "sparkMaster", doc="URL of the Spark Master to submit jobs to when using the Spark pipeline runner.", optional = true)
    protected String sparkMaster;

    @Override
    protected Object doWork() {
        final JavaSparkContext ctx = buildContext();
        runPipeline(ctx);
        afterPipeline(ctx);

        return null;
    }

    private JavaSparkContext buildContext() {
        String masterUrl = "local[" + Integer.toString(numWorkers) + "]";
        if (sparkMaster != null) {
            masterUrl = sparkMaster;
        }

        SparkConf sparkConf = new SparkConf().setAppName(getProgramName())
                .setMaster(masterUrl)
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "org.broadinstitute.hellbender.engine.spark.GATKRegistrator");

        if (apiKey != null) {
            sparkConf.set("apiKey", apiKey);
        }
        if (clientSecret != null) {
            sparkConf.set("client_secret", clientSecret.getAbsolutePath());
        }

        return new JavaSparkContext(sparkConf);
    }

    // ---------------------------------------------------
    // Functions meant for overriding

    protected abstract void runPipeline(final JavaSparkContext ctx);

    /**
     * Override this to run code after the pipeline returns.
     */
    protected void afterPipeline(final JavaSparkContext ctx) {
        ctx.close();
    }

    protected abstract String getProgramName();
    // ---------------------------------------------------
    // Helpers

}