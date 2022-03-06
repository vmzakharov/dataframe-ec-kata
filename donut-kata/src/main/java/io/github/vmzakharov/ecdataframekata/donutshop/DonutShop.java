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
        //  Hint: Look at selectBy(), aggregate() and the AggregateFunction class

        return this.getOrdersWithPrices()
            .selectBy("DeliveryDate >= toDate('" + fromDate + "') and DeliveryDate <= toDate('" + toDate + "')")
            .aggregate(Lists.immutable.of(
                sum("TotalPrice"),
                avg("TotalPrice", "Average Price"),
                count("TotalPrice", "Order Count"),
                min("TotalPrice", "Min Price"),
                max("TotalPrice", "Max Price"))
            );
    }

    public DataFrame getOrdersWithPrices()
    {
        if (!this.orders.hasColumn("TotalPrice"))
        {
            this.orders.lookup(DfJoin
                    .to(this.menu)
                    .match("DonutCode", "Code")
                    .select(Lists.immutable.of("Price", "DiscountPrice"))
            );

            this.orders.addDoubleColumn("TotalPrice", "(Count < 10 ? Price : DiscountPrice)  * Count");
        }

        return this.orders;
    }
}
