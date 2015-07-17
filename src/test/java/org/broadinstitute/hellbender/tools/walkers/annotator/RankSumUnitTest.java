package org.broadinstitute.hellbender.tools.walkers.annotator;

import org.apache.commons.math3.stat.inference.MannWhitneyUTest;
import org.apache.commons.math3.stat.ranking.NaNStrategy;
import org.apache.commons.math3.stat.ranking.TiesStrategy;
import org.broadinstitute.hellbender.utils.Utils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class RankSumUnitTest {

    List<Integer> distribution20, distribution30, distribution20_40;
    static final int observations = 100;

    @BeforeClass
    public void init() {
        distribution20 = new ArrayList<>(observations);
        distribution30 = new ArrayList<>(observations);
        distribution20_40 = new ArrayList<>(observations);

        final int skew = 3;
        makeDistribution(distribution20, 20, skew, observations);
        makeDistribution(distribution30, 30, skew, observations);
        makeDistribution(distribution20_40, 20, skew, observations/2);
        makeDistribution(distribution20_40, 40, skew, observations/2);

        // shuffle the observations
        Collections.shuffle(distribution20, Utils.getRandomGenerator());
        Collections.shuffle(distribution30, Utils.getRandomGenerator());
        Collections.shuffle(distribution20_40, Utils.getRandomGenerator());
    }

    private static void makeDistribution(final List<Integer> result, final int target, final int skew, final int numObservations) {
        final int rangeStart = target - skew;
        final int rangeEnd = target + skew;

        int current = rangeStart;
        for ( int i = 0; i < numObservations; i++ ) {
            result.add(current++);
            if ( current > rangeEnd )
                current = rangeStart;
        }
    }

    @DataProvider(name = "DistributionData")
    public Object[][] makeDistributionData() {
        List<Object[]> tests = new ArrayList<>();

        for ( final int numToReduce : Arrays.asList(0, 10, 50, 100) ) {
            tests.add(new Object[]{distribution20, distribution20, numToReduce, true, "20-20"});
            tests.add(new Object[]{distribution30, distribution30, numToReduce, true, "30-30"});
            tests.add(new Object[]{distribution20_40, distribution20_40, numToReduce, true, "20/40-20/40"});

            tests.add(new Object[]{distribution20, distribution30, numToReduce, false, "20-30"});
            tests.add(new Object[]{distribution30, distribution20, numToReduce, false, "30-20"});

            tests.add(new Object[]{distribution20, distribution20_40, numToReduce, false, "20-20/40"});
            tests.add(new Object[]{distribution30, distribution20_40, numToReduce, true, "30-20/40"});
        }

        return tests.toArray(new Object[][]{});
    }

    @Test(enabled = true, dataProvider = "DistributionData")
    public void testDistribution(final List<Integer> distribution1, final List<Integer> distribution2, final int numToReduceIn2, final boolean distributionsShouldBeEqual, final String debugString) {
        final MannWhitneyUTest mwu = new MannWhitneyUTest(NaNStrategy.FIXED, TiesStrategy.RANDOM);

        final double[] set1 = new double[distribution1.size()];
        for (int i = 0; i < distribution1.size(); i++){
            set1[i] = distribution1.get(i);
        }

        final List<Integer> dist2 = new ArrayList<>(distribution2);
        if ( numToReduceIn2 > 0 ) {
            int counts = 0;
            int quals = 0;

            for ( int i = 0; i < numToReduceIn2; i++ ) {
                counts++;
                quals += dist2.remove(0);
            }

            final int qual = quals / counts;
            for ( int i = 0; i < numToReduceIn2; i++ )
                dist2.add(qual);
        }

        final double[] set2 = new double[distribution2.size()];
        for (int i = 0; i < distribution2.size(); i++){
            set2[i] = distribution2.get(i);
        }
        final double result = mwu.mannWhitneyUTest(set1, set2);
        Assert.assertFalse(Double.isNaN(result));

        if ( distributionsShouldBeEqual ) {
            // TODO -- THIS IS THE FAILURE POINT OF USING REDUCED READS WITH RANK SUM TESTS
            if ( numToReduceIn2 >= observations / 2 )
                return;
            Assert.assertTrue(result > 0.1, String.format("%f %d %d", result, numToReduceIn2, dist2.get(0)));
        } else {
            Assert.assertTrue(result < 0.01, String.format("%f %d %d", result, numToReduceIn2, dist2.get(0)));
        }
    }
}
