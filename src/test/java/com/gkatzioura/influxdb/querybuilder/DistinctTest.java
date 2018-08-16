package com.gkatzioura.influxdb.querybuilder;

import org.junit.jupiter.api.Test;

import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.select;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DistinctTest {

    @Test
    public void testDistinct() {
        String query = "SELECT DISTINCT k FROM foo;";
        Statement select = select().column("k").distinct().from("foo");
        assertEquals(query, select.toString());
    }

    @Test
    public void testDistinctWithExpression() {
        String query = "SELECT DISTINCT COUNT(test1) FROM foo LIMIT 1 OFFSET 20;";
        Statement select = select().count("test1").distinct().from("foo").limit(1, 20);
        assertEquals(query, select.toString());
    }

    @Test
    public void testMultipleColumns() {

    }

}
