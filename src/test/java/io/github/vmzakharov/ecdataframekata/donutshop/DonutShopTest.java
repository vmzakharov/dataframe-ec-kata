package io.github.vmzakharov.ecdataframekata.donutshop;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import org.junit.jupiter.api.BeforeEach;

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
                    .addStringColumn("Code").addStringColumn("Type").addDoubleColumn("Price").addDoubleColumn("Discount Price")
                    .addRow("BB", "BLUEBERRY", 1.50, 1.25)
                    .addRow("GL", "GLAZED", 1.50, 1.25)
                    .addRow("OF", "OLD_FASHIONED", 1.50, 1.25)
                    .addRow("P",  "PUMPKIN", 1.50, 1.25)
                    .addRow("J",  "JELLY", 1.50, 1.25)
                    .addRow("AC", "APPLE_CIDER", 1.50, 1.25)
        );

        this.donutShop.setOrders(
                new DataFrame("Orders")
                    .addStringColumn("CustomerId").addDateColumn("Date").addStringColumn("DonutCode").addLongColumn("Count")
                    .addRow(1, this.today, "BB", 2)
                    .addRow(1, this.today, "BB", 2)
                    .addRow(1, this.today, "BB", 2)
                    .addRow(2, this.today, "BB", 2)
                    .addRow(2, this.today, "BB", 2)
                    .addRow(3, this.yesterday, "BB", 2)
                    .addRow(3, this.yesterday, "BB", 2)
                    .addRow(3, this.tomorrow, "BB", 2)
                    .addRow(3, this.tomorrow, "BB", 2)
                    .addRow(4, this.tomorrow, "BB", 2)
                    .addRow(4, this.tomorrow, "BB", 2)
                    .addRow(4, this.tomorrow, "BB", 2)
                    .addRow(4, this.tomorrow, "BB", 2)
        );
    }
}
