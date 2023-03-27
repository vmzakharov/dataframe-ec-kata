package io.github.vmzakharov.ecdataframekata.util;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.util.DataFrameCompare;
import org.junit.jupiter.api.Assertions;

final public class DataFrameUtil
{
    private DataFrameUtil()
    {
        // Utility class
    }

    static public void assertEquals(DataFrame expected, DataFrame actual)
    {
        DataFrameCompare comparer = new DataFrameCompare();

        if (!comparer.equal(expected, actual))
        {
            Assertions.fail(comparer.reason());
        }
    }
}
