package io.github.vmzakharov.ecdataframekata.donutshop;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfJoin;
import org.eclipse.collections.api.factory.Lists;

import java.time.LocalDate;

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.avg;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.count;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.max;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.min;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.sum;

public class DonutShop
{
    private DataFrame orders;
    private DataFrame customers;
    private DataFrame menu;

    public DataFrame getCustomers()
    {
        return this.customers;
    }

    public void setCustomers(DataFrame newCustomers)
    {
        this.customers = newCustomers;
    }

    public void setMenu(DataFrame newMenu)
    {
        this.menu = newMenu;
    }

    public DataFrame getMenu()
    {
        return this.menu;
    }

    public DataFrame getOrders()
    {
        return this.orders;
    }

    public void setOrders(DataFrame newOrders)
    {
        this.orders = newOrders;
    }

    public DataFrame getDonutOrderPriceStatistics(LocalDate fromDate, LocalDate toDate)
    {
        // TODO - Calculate the statistics fpr the deliveries for inclusive of the specified date range.
        //  For orders with the deliveries withing the range calculate the following statistics:
        //   * TotalPrice
        //   * Average Price
        //   * Order Count
        //   * Min Price
        //   * Max Price
        //
        // Hint: Look at the selectBy(), aggregate() methods and the AggregateFunction class
        // the expression DSL has a built-in toDate() function that may be used inline date parameters in the expression

        return null;
    }

    public DataFrame getOrdersWithPrices()
    {
        // TODO - for the orders data frame perform a lookup on the menu data frame that contains regular price and
        //  discount price for each donut kind. With the prices available on the order data frame calculate the total
        //  price for each order as follows:
        //  - if the quantity of a particular kind of donut is less than 10 then use the regular price to calculate the
        //  total, otherwise use the discount price
        //
        // Hint: look at the lookup() method and the DfJoin class, which defines lookup parameters.
        // Look at the addDoubleColumn() method with the expression parameter to add a calculated value to a dataframe.
        // It is possible that this method will be called more than once so use the hasColumn() method to check if
        // calculated column has been added yet.

        return null;
    }
}
