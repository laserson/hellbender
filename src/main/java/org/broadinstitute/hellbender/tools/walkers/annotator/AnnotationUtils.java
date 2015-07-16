package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.Genotype;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.walkers.annotator.interfaces.AnnotatorCompatible;
import org.broadinstitute.hellbender.utils.genotyper.PerReadAlleleLikelihoodMap;

public class AnnotationUtils {

    /**
     * Checks if the input data is appropriate
     *
     * @param walker input walker
     * @param map input map for each read, holds underlying alleles represented by an aligned read, and corresponding relative likelihood.
     * @param g input genotype
     * @param warningsLogged array that enforces the warning is logged once for each caller
     * @param logger logger specific for each caller
     *
     * @return true if the walker is a HaplotypeCaller, the likelihood map is non-null and the genotype is non-null and called, false otherwise
     * @throws ReviewedGATKException if the size of warningsLogged is less than 4.
     */
    public static boolean isAppropriateInput(final AnnotatorCompatible walker, final PerReadAlleleLikelihoodMap map, final Genotype g, final boolean[] warningsLogged, final Logger logger) {

        if ( warningsLogged.length < 4 ){
            throw new GATKException("Warnings logged array must have at last 4 elements, but has " + warningsLogged.length);
        }

        if ( !(walker instanceof HaplotypeCaller) ) {
            if ( !warningsLogged[0] ) {
                if ( walker != null ) {
                    logger.warn("Annotation will not be calculated, must be called from HaplotyepCaller, not " + walker.getClass().getName());
                } else {
                    logger.warn("Annotation will not be calculated, must be called from HaplotyepCaller");
                }
                warningsLogged[0] = true;
            }
            return false;
        }

        if ( map == null ){
            if ( !warningsLogged[1] ) {
                logger.warn("Annotation will not be calculated, can only be used with likelihood based annotations in the HaplotypeCaller");
                warningsLogged[1] = true;
            }
            return false;
        }

        if ( g == null ){
            if ( !warningsLogged[2] ) {
                logger.warn("Annotation will not be calculated, missing genotype");
                warningsLogged[2]= true;
            }
            return false;
        }

        if ( !g.isCalled() ){
            if ( !warningsLogged[3] ) {
                logger.warn("Annotation will not be calculated, genotype is not called");
                warningsLogged[3] = true;
            }
            return false;
        }

        return true;
    }
}
