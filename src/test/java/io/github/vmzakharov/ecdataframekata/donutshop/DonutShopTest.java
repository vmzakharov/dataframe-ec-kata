package io.github.vmzakharov.ecdataframekata.donutshop;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfJoin;
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
                    .addStringColumn("Code").addStringColumn("Type").addDoubleColumn("Price").addDoubleColumn("DiscountPrice")
                    .addRow("BB", "BLUEBERRY", 1.75, 1.50)
                    .addRow("GL", "GLAZED", 1.50, 1.25)
                    .addRow("OF", "OLD_FASHIONED", 1.50, 1.25)
                    .addRow("PS", "PUMPKIN_SPICE", 1.75, 1.50)
                    .addRow("JL",  "JELLY", 1.75, 1.50)
                    .addRow("AC", "APPLE_CIDER", 1.50, 1.25)
        );

        this.donutShop.setOrders(
                new DataFrame("Orders")
                    .addLongColumn("CustomerId").addDateColumn("Date").addStringColumn("DonutCode").addLongColumn("Count")
                    .addRow(1, this.today, "BB", 2)
                    .addRow(1, this.today, "GL", 2)
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

        this.donutShop.getOrders()
              .addDoubleColumn("Total", "(Count < 10 ? Price : DiscountPrice)  * Count");

        System.out.println(this.donutShop.getOrders().asCsvString());

        var spendByCustomer =
                this.donutShop.getOrders().sumBy(Lists.immutable.of("Total"), Lists.immutable.of("CustomerId"));

        System.out.println(spendByCustomer.asCsvString());

        spendByCustomer.sortByExpression("-Total");
        spendByCustomer.lookup(DfJoin
                        .to(this.donutShop.getCustomers())
                        .match("CustomerId", "Id")
                        .select("Name")
        );

        System.out.println(spendByCustomer.asCsvString());
        System.out.println(spendByCustomer.getString("Name", 0));

        var tomorrowsOrders = this.donutShop.getOrders().selectBy("Date == toDate('" + this.tomorrow + "')");
        System.out.println(tomorrowsOrders.asCsvString());
        // customer names for delivery tomorrow

        Assertions.assertEquals(
                Sets.mutable.of("Carol", "Dave"),
                tomorrowsOrders.lookup(DfJoin
                    .to(this.donutShop.getCustomers())
                    .match("CustomerId", "Id")
                    .select("Name")
                ).getStringColumn("Name").toList().toSet());
    }

    @Test
    public void totalPriceForEachOrder()
    {

    }
}
