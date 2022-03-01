package io.github.vmzakharov.ecdataframekata.donutshop;

import io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfJoin;
import io.github.vmzakharov.ecdataframekata.util.DataFrameUtil;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

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
        this.donutShop.setCustomers(
                new DataFrame("Customers")
                    .addLongColumn("Id").addStringColumn("Name")
                    .addRow(1, "Alice")
                    .addRow(2, "Bob")
                    .addRow(3, "Carol")
                    .addRow(4, "Dave")
        );

        this.donutShop.setMenu(
                new DataFrame("Menu")
                    .addStringColumn("Code").addStringColumn("Description").addDoubleColumn("Price").addDoubleColumn("DiscountPrice")
                    .addRow("BB", "Blueberry", 1.75, 1.50)
                    .addRow("GL", "Glazed", 1.50, 1.25)
                    .addRow("OF", "Old Fashioned", 1.50, 1.25)
                    .addRow("PS", "Pumpkin Spice", 1.75, 1.50)
                    .addRow("JL", "Jelly", 1.75, 1.50)
                    .addRow("AC", "Apple Cider", 1.50, 1.25)
        );

        this.donutShop.setOrders(
                new DataFrame("Orders")
                    .addLongColumn("CustomerId").addDateColumn("DeliveryDate").addStringColumn("DonutCode").addLongColumn("Count")
                    .addRow(1, this.today, "BB", 2)
                    .addRow(1, this.today, "GL", 4)
                    .addRow(1, this.today, "OF", 10)

                    .addRow(2, this.yesterday, "BB", 12)
                    .addRow(2, this.today, "PS", 12)

                    .addRow(3, this.yesterday, "JL", 2)
                    .addRow(3, this.tomorrow, "OF", 8)
                    .addRow(3, this.tomorrow, "BB", 12)

                    .addRow(4, this.yesterday, "PS", 2)
                    .addRow(4, this.today, "OF", 12)
                    .addRow(4, this.tomorrow, "OF", 10)
        );
    }

    @Test
    public void totalSpendPerCustomer()
    {
        this.donutShop.getOrders().lookup(DfJoin
                .to(this.donutShop.getMenu())
                .match("DonutCode", "Code")
                .select(Lists.immutable.of("Price", "DiscountPrice"))
        );

        this.donutShop.getOrders().addDoubleColumn("TotalPrice", "(Count < 10 ? Price : DiscountPrice)  * Count");

        System.out.println(this.donutShop.getOrders().asCsvString());

        var spendByCustomer =
                this.donutShop.getOrders().sumBy(Lists.immutable.of("TotalPrice"), Lists.immutable.of("CustomerId"));

        System.out.println(spendByCustomer.asCsvString());

        spendByCustomer.sortByExpression("-TotalPrice");
        spendByCustomer.lookup(DfJoin
                .to(this.donutShop.getCustomers())
                .match("CustomerId", "Id")
                .select("Name")
        );

        System.out.println(spendByCustomer.asCsvString());
        System.out.println(spendByCustomer.getString("Name", 0));

        var tomorrowsOrders = this.donutShop.getOrders().selectBy("DeliveryDate == toDate('" + this.tomorrow + "')");
        System.out.println(tomorrowsOrders.asCsvString());
        // customer names for delivery tomorrow

        Assertions.assertEquals(
                Sets.mutable.of("Carol", "Dave"),
                tomorrowsOrders.lookup(DfJoin
                        .to(this.donutShop.getCustomers())
                        .match("CustomerId", "Id")
                        .select("Name")
                ).getStringColumn("Name").toList().toSet());

        // statistics by delivery date range - sum, avg, etc.
        // var donutDeliveryStatistics = this.donutShop.getStatistics.
    }

    @Test
    public void donutPriceStatistics()
    {
        this.donutShop.getOrders().lookup(DfJoin
                .to(this.donutShop.getMenu())
                .match("DonutCode", "Code")
                .select(Lists.immutable.of("Price", "DiscountPrice"))
        );

        this.donutShop.getOrders().addDoubleColumn("TotalPrice", "(Count < 10 ? Price : DiscountPrice)  * Count");

        DataFrame statistics;

        statistics = this.donutShop.getDonutPriceStatistics(this.yesterday, this.yesterday);
        System.out.println(statistics.asCsvString());

        statistics = this.donutShop.getDonutPriceStatistics(this.today, this.today);
        System.out.println(statistics.asCsvString());

        statistics = this.donutShop.getDonutPriceStatistics(this.tomorrow, this.tomorrow);
        System.out.println(statistics.asCsvString());

        DataFrameUtil.assertEquals(
                new DataFrame("Expected")
                        .addDoubleColumn("Sum").addDoubleColumn("Avg").addLongColumn("Count")
                        .addDoubleColumn("Min").addDoubleColumn("Max")
                        .addRow(122.5, 11.136363636363637, 86, 3.5, 18.0),
                this.donutShop.getDonutPriceStatistics(this.yesterday, this.tomorrow));
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

        DataFrameUtil.assertEquals(
                new DataFrame("Expected")
                    .addLongColumn("Count").addStringColumn("Description")
                    .addRow(40, "Old Fashioned")
                    .addRow(26, "Blueberry")
                    .addRow(14, "Pumpkin Spice")
                    .addRow(4, "Glazed")
                    .addRow(2, "Jelly"),
                popularity
        );
    }
}
