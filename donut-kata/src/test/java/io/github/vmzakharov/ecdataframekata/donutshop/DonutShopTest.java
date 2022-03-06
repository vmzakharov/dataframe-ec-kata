package io.github.vmzakharov.ecdataframekata.donutshop;

import io.github.vmzakharov.ecdataframekata.util.DataFrameUtil;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfJoin;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

public class DonutShopTest
{
    private final Clock clock = Clock.fixed(Instant.now(), ZoneOffset.UTC);
    private final LocalDate today = LocalDate.now(this.clock);
    private final LocalDate tomorrow = this.today.plusDays(1);
    private final LocalDate yesterday = this.today.minusDays(1);

    private DonutShop donutShop;

    @BeforeEach
    public void setup()
    {
        this.donutShop = new DonutShop();

        this.donutShop.setCustomers(new DataFrame("Customers")
                .addLongColumn("Id").addStringColumn("Name")
                .addRow(1, "Alice")
                .addRow(2, "Bob")
                .addRow(3, "Carol")
                .addRow(4, "Dave")
        );

        this.donutShop.setMenu(new DataFrame("Menu")
                .addStringColumn("Code").addStringColumn("Description").addDoubleColumn("Price").addDoubleColumn("DiscountPrice")
                .addRow("BB", "Blueberry", 2.00, 1.75)
                .addRow("GL", "Glazed", 1.50, 1.25)
                .addRow("OF", "Old Fashioned", 1.50, 1.25)
                .addRow("PS", "Pumpkin Spice", 2.00, 1.75)
                .addRow("JL", "Jelly", 2.00, 1.75)
                .addRow("AC", "Apple Cider", 1.50, 1.25)
        );

        this.donutShop.setOrders(new DataFrame("Orders")
                .addLongColumn("CustomerId").addDateColumn("DeliveryDate").addStringColumn("DonutCode").addLongColumn("Count")
                .addRow(1, this.today, "BB", 2)
                .addRow(1, this.today, "GL", 4)
                .addRow(1, this.today, "OF", 10)

                .addRow(2, this.yesterday, "BB", 12)
                .addRow(2, this.today, "PS", 12)

                .addRow(3, this.yesterday, "OF", 1)
                .addRow(3, this.yesterday, "JL", 2)
                .addRow(3, this.tomorrow, "OF", 10)
                .addRow(3, this.tomorrow, "GL", 1)
                .addRow(3, this.tomorrow, "BB", 12)

                .addRow(4, this.yesterday, "PS", 2)
                .addRow(4, this.today, "OF", 12)
                .addRow(4, this.tomorrow, "OF", 10)
        );
    }

    @Test
    public void totalSpendPerCustomer()
    {
        // TODO - find the total spend per customer. Create a data frame containing one row per each customer with two
        //  columns: total price of all of their orders and the customer's name

        var spendByCustomer = this.donutShop
                .getOrdersWithPrices()
                .sumBy(Lists.immutable.of("TotalPrice"), Lists.immutable.of("CustomerId"));

        spendByCustomer
            .lookup(DfJoin
                .to(this.donutShop.getCustomers())
                .match("CustomerId", "Id")
                .select("Name")
                )
            .sortByExpression("-TotalPrice");

        spendByCustomer.dropColumn("CustomerId");

        DataFrameUtil.assertEquals(
                new DataFrame("Expected")
                        .addDoubleColumn("TotalPrice").addStringColumn("Name")
                        .addRow(42.0,"Bob")
                        .addRow(40.5,"Carol")
                        .addRow(31.5,"Dave")
                        .addRow(22.5,"Alice"),
                spendByCustomer
        );
    }

    @Test
    public void customersWithDeliveriesTomorrow()
    {
        // TODO - find the names of the customers with the deliveries scheduled for tomorrow
        var tomorrowsOrders = this.donutShop.getOrders().selectBy("DeliveryDate == toDate('" + this.tomorrow + "')");

        var deliveriesTomorrow =
            tomorrowsOrders.lookup(DfJoin
                .to(this.donutShop.getCustomers())
                .match("CustomerId", "Id")
                .select("Name")
        ).getStringColumn("Name").toList().toSet();

        assertEquals(Sets.mutable.of("Carol", "Dave"), deliveriesTomorrow);
    }

    @Test
    public void donutPriceStatistics()
    {
        var statistics1 = this.donutShop.getDonutOrderPriceStatistics(this.yesterday, this.yesterday);

        assertEquals(30.5, statistics1.getDouble("TotalPrice", 0));
        assertEquals(7.625, statistics1.getDouble("Average Price", 0));
        assertEquals(4, statistics1.getLong("Order Count", 0));
        assertEquals(1.5, statistics1.getDouble("Min Price", 0));
        assertEquals(21.0, statistics1.getDouble("Max Price", 0));

        var statistics2 = this.donutShop.getDonutOrderPriceStatistics(this.today, this.today);

        assertEquals(58.5, statistics2.getDouble("TotalPrice", 0));
        assertEquals(11.7, statistics2.getDouble("Average Price", 0));
        assertEquals(5, statistics2.getLong("Order Count", 0));
        assertEquals(4.0, statistics2.getDouble("Min Price", 0));
        assertEquals(21.0, statistics2.getDouble("Max Price", 0));

        var statistics3 = this.donutShop.getDonutOrderPriceStatistics(this.tomorrow, this.tomorrow);

        assertEquals(47.5, statistics3.getDouble("TotalPrice", 0));
        assertEquals(11.875, statistics3.getDouble("Average Price", 0));
        assertEquals(4, statistics3.getLong("Order Count", 0));
        assertEquals(1.5, statistics3.getDouble("Min Price", 0));
        assertEquals(21.0, statistics3.getDouble("Max Price", 0));

        var statistics4 = this.donutShop.getDonutOrderPriceStatistics(this.yesterday, this.tomorrow);

        assertEquals(136.5, statistics4.getDouble("TotalPrice", 0));
        assertEquals(10.5, statistics4.getDouble("Average Price", 0));
        assertEquals(13, statistics4.getLong("Order Count", 0));
        assertEquals(1.5, statistics4.getDouble("Min Price", 0));
        assertEquals(21.0, statistics4.getDouble("Max Price", 0));
    }

    @Test
    public void donutsInPopularityOrder()
    {
        // TODO - create a data frame that contains donut kinds (descriptions) and the number of donuts delivered,
        //        the data frame should be ordered by the number of donuts value in the descending order

        var donutCountByCode = this.donutShop.getOrders().sumBy(Lists.immutable.of("Count"), Lists.immutable.of("DonutCode"));

        var popularity = donutCountByCode.lookup(DfJoin
                .to(this.donutShop.getMenu())
                .match("DonutCode", "Code")
                .select("Description")
        );

        popularity.dropColumn("DonutCode").sortByExpression("-Count");

        DataFrameUtil.assertEquals(new DataFrame("Expected")
                        .addLongColumn("Count").addStringColumn("Description")
                        .addRow(43, "Old Fashioned")
                        .addRow(26, "Blueberry")
                        .addRow(14, "Pumpkin Spice")
                        .addRow(5, "Glazed")
                        .addRow(2, "Jelly"),
                popularity
        );
    }
}
