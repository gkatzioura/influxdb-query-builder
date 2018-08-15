package com.gkatzioura.influxdb.querybuilder;

import org.junit.jupiter.api.Test;

import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.select;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AliasTest {

    @Test
    public void testAlias() {
        String query = "SELECT MAX(k) AS hello FROM foo;";
        Statement select = select().max("k").as("hello").from("foo");
        assertEquals(query, select.toString());
    }

}
