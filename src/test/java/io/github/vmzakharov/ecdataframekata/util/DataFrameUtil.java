package io.github.vmzakharov.ecdataframekata.util;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import org.junit.jupiter.api.Assertions;

final public class DataFrameUtil
{
    private DataFrameUtil()
    {
        // Utility class
    }

    static public void assertEquals(DataFrame expected, DataFrame actual)
    {

        Assertions.assertTrue(
                expected.rowCount() == actual.rowCount() && expected.columnCount() == actual.columnCount(),
                "Dimensions don't match: expected rows " + expected.rowCount() + ", cols " + expected.columnCount()
                    + ", actual rows " + actual.rowCount() + ", cols " + actual.columnCount());

        Assertions.assertEquals(
                expected.getColumns().collect(DfColumn::getName), actual.getColumns().collect(DfColumn::getName),
                "Column names mismatch");

        int colCount = expected.columnCount();
        int rowCount = expected.rowCount();

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            for (int colIndex = 0; colIndex < colCount; colIndex++)
            {
                Assertions.assertEquals(
                        expected.getObject(rowIndex, colIndex), actual.getObject(rowIndex, colIndex),
                        "Different values in row " + rowIndex + ", column " + colIndex);
            }
        }
    }
}
