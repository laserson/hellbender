package org.broadinstitute.hellbender.tools.walkers.genotyper.afcalc;

/**
 * Factory to make AFCalculations
 */
public final class AFCalculatorFactory {



    /**
     * Create a new AFCalc, choosing the best implementation based on the given parameters, assuming
     * that we will only be requesting bi-allelic variants to diploid genotypes
     *
     * @return an initialized AFCalc
     *
     * @deprecated hardcoded special reference to diploid analysis... eventually using code must explicitly indicate the
     *  ploidy as this is a sign of a hidden diploid assumption which is bad.
     */
    @Deprecated
    public static AFCalculator createCalculatorForDiploidBiAllelicAnalysis() {
        return AFCalculatorImplementation.bestValue(2,1,null).newInstance();
    }

    /**
     * Create a new AFCalc, choosing the best implementation based on the given parameters
     *
     * @param nSamples the number of samples we'll be using
     * @param maxAltAlleles the max. alt alleles to consider for SNPs
     * @param ploidy the sample ploidy.  Must be consistent with the calc
     *
     * @return an initialized AFCalc
     */
    public static AFCalculator createCalculator(final int nSamples, final int maxAltAlleles, final int ploidy) {
        return createCalculator(chooseBestImplementation(ploidy, maxAltAlleles), nSamples, maxAltAlleles, ploidy);
    }

    /**
     * Choose the best calculation for nSamples and ploidy
     *
     * @param ploidy
     * @param maxAltAlleles
     * @return
     */
    private static AFCalculatorImplementation chooseBestImplementation(final int ploidy, final int maxAltAlleles) {
        for ( final AFCalculatorImplementation calc : AFCalculatorImplementation.values() ) {
            if ( calc.usableForParams(ploidy, maxAltAlleles) ) {
                return calc;
            }
        }

        throw new IllegalStateException("no calculation found that supports nSamples " + ploidy + " and maxAltAlleles " + maxAltAlleles);
    }

    /**
     * Create a new AFCalc
     *
     * @param implementation the calculation to use
     * @param nSamples the number of samples we'll be using
     * @param maxAltAlleles the max. alt alleles to consider for SNPs
     * @param ploidy the sample ploidy.  Must be consistent with the implementation
     *
     * @return an initialized AFCalc
     */
    public static AFCalculator createCalculator(final AFCalculatorImplementation implementation, final int nSamples, final int maxAltAlleles, final int ploidy) {
        if ( implementation == null ) throw new IllegalArgumentException("Calculation cannot be null");
        if ( nSamples < 0 ) throw new IllegalArgumentException("nSamples must be greater than zero " + nSamples);
        if ( maxAltAlleles < 1 ) throw new IllegalArgumentException("maxAltAlleles must be greater than zero " + maxAltAlleles);
        if ( ploidy < 1 ) throw new IllegalArgumentException("sample ploidy must be greater than zero " + ploidy);

        if ( ! implementation.usableForParams(ploidy, maxAltAlleles) )
            throw new IllegalArgumentException("AFCalc " + implementation + " does not support requested ploidy " + ploidy);

        return implementation.newInstance();
    }

}
