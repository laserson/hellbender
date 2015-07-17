package org.broadinstitute.hellbender.tools.walkers.annotator;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StrandOddsRatioUnitTest {
    private static double DELTA_PRECISION = 0.001;

    @DataProvider(name = "UsingSOR")
    public Object[][] makeUsingSORData() {
        final double LOG_OF_TWO =  0.6931472;
        List<Object[]> tests = new ArrayList<>();
        tests.add(new Object[]{0, 0, 0, 0, LOG_OF_TWO});
        tests.add(new Object[]{100000, 100000, 100000, 100000, LOG_OF_TWO}  );
        tests.add(new Object[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, LOG_OF_TWO}  );

        tests.add(new Object[]{0, 0, 100000, 100000, LOG_OF_TWO});
        tests.add(new Object[]{0, 0, Integer.MAX_VALUE, Integer.MAX_VALUE, LOG_OF_TWO});

        tests.add(new Object[]{100000,100000,100000,0, 23.02587});
        tests.add(new Object[]{100,100,100,0, 9.230339});
        tests.add(new Object[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE,0, 42.97513});

        tests.add(new Object[]{13736,9047,41,1433, 7.061479});
        tests.add(new Object[]{66, 14, 64, 4, 2.248203});
        tests.add(new Object[]{351169, 306836, 153739, 2379, 8.066731});
        tests.add(new Object[]{116449, 131216, 289, 16957, 7.898818});
        tests.add(new Object[]{137, 159, 9, 23, 1.664854});
        tests.add(new Object[]{129, 90, 21, 20, 0.4303384});
        tests.add(new Object[]{14054, 9160, 16, 7827, 12.2645});
        tests.add(new Object[]{32803, 9184, 32117, 3283, 2.139932});
        tests.add(new Object[]{2068, 6796, 1133, 0, 14.06701});

        return tests.toArray(new Object[][]{});
    }

    @Test(dataProvider = "UsingSOR")
    public void testUsingSOR(final int refpos, final int refneg, final int altpos, final int altneg, double expectedOddsRatio ) {
        int[][] contingencyTable = new int[2][2];
        contingencyTable[0][0] = refpos;
        contingencyTable[0][1] = refneg;
        contingencyTable[1][0] = altpos;
        contingencyTable[1][1] = altneg;
        final double ratio = new StrandOddsRatio(Collections.emptySet()).calculateSOR(contingencyTable);
        Assert.assertEquals(ratio, expectedOddsRatio, DELTA_PRECISION, "Pass");
    }
}
