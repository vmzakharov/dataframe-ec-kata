package io.github.vmzakharov.ecdataframekata.donutshop;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;

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
}
