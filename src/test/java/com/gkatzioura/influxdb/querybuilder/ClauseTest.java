package com.gkatzioura.influxdb.querybuilder;

import org.junit.jupiter.api.Test;

import com.gkatzioura.influxdb.querybuilder.statement.Statement;

import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.contains;
import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.regex;
import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.select;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClauseTest {

    @Test
    public void testRegex() {

        String query = "SELECT MAX(k) FROM foo WHERE k =~ /[0-9]/;";
        Statement select = select().max("k").from("foo").where(regex("k", "/[0-9]/"));
        assertEquals(query,select.toString());
    }

    @Test
    public void testContains() {

        String query = "SELECT MAX(k) FROM foo WHERE k =~ /*text*/;";
        Statement select = select().max("k").from("foo").where(contains("k", "text"));
        assertEquals(query,select.toString());
    }

}
