package io.github.vmzakharov.ecdataframekata.donutshop;

import io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import org.eclipse.collections.api.factory.Lists;

import java.time.LocalDate;

public class DonutShop
{
    private DataFrame inventory;
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

    public DataFrame getInventory()
    {
        return this.inventory;
    }

    public void setInventory(DataFrame newInventory)
    {
        this.inventory = newInventory;
    }

    public DataFrame getOrders()
    {
        return this.orders;
    }

    public void setOrders(DataFrame newOrders)
    {
        this.orders = newOrders;
    }

    public DataFrame getDonutPriceStatistics(LocalDate fromDate, LocalDate toDate)
    {
        return this.getOrders()
            .selectBy("DeliveryDate >= toDate('" + fromDate + "') and DeliveryDate <= toDate('" + toDate + "')")
            .aggregate(Lists.immutable.of(
                AggregateFunction.sum("TotalPrice", "Sum"),
                AggregateFunction.avg("TotalPrice", "Avg"),
                AggregateFunction.sum("Count", "Count"),
                AggregateFunction.min("TotalPrice", "Min"),
                AggregateFunction.max("TotalPrice", "Max"))
            );
    }
}
