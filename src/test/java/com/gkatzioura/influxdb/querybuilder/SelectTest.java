/**
 * MIT License
 *
 * Copyright (c) 2018 Emmanouil Gkatziouras
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE
 */

package com.gkatzioura.influxdb.querybuilder;

import org.junit.jupiter.api.Test;

import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.select;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SelectTest {

    @Test
    public void testSelect() {

        String query = "SELECT * FROM foo;";
        Statement select = select().all().from("foo");
        assertEquals(query,select.toString());
    }

    @Test
    public void testSelectField() {

        String query = "SELECT test1,test2 FROM foo;";
        Statement select = select().column("test1").column("test2") .from("foo");
        assertEquals(query, select.toString());
    }

    @Test
    public void testGroupBy() {

        String query = "SELECT test1 FROM foo GROUP BY test2,test3;";
        Statement select = select().column("test1") .from("foo").groupBy("test2","test3");
        assertEquals(query,select.toString());
    }

    @Test
    public void testLimit() {

        String query = "SELECT test1 FROM foo GROUP BY test2,test3 LIMIT 1;";
        Statement select = select().column("test1").from("foo").groupBy("test2","test3").limit(1);
        assertEquals(query,select.toString());
    }

    @Test
    public void testLimitOffset() {

        String query = "SELECT test1 FROM foo GROUP BY test2,test3 LIMIT 1 OFFSET 20;";
        Statement select = select().column("test1").from("foo").groupBy("test2","test3").limit(1,20);
        assertEquals(query,select.toString());
    }

    @Test
    public void testFailedDistinct() {

        assertThrows(IllegalStateException.class,() -> select().column("test1").column("test2").distinct("test3").from("foo"));
    }

    @Test
    public void testDistinct() {

        String query = "SELECT DISTINCT test1 FROM foo LIMIT 1 OFFSET 20;";
        Statement select = select().distinct("test1").from("foo").limit(1,20);
        assertEquals(query,select.toString());
    }

    @Test
    public void testCount() {

        String query = "SELECT COUNT(test1) FROM foo LIMIT 1 OFFSET 20;";
        Statement select = select().count("test1").from("foo").limit(1,20);
        assertEquals(query,select.toString());
    }

    @Test
    public void testMin() {

        String query = "SELECT MIN(test1) FROM foo LIMIT 1 OFFSET 20;";
        Statement select = select().min("test1").from("foo").limit(1,20);
        assertEquals(query,select.toString());
    }

    @Test
    public void testMax() {
        String query = "SELECT MAX(test1) FROM foo LIMIT 1 OFFSET 20;";
        Statement select = select().max("test1").from("foo").limit(1,20);
        assertEquals(query,select.toString());
    }

    @Test
    public void testSum() {

        String query = "SELECT SUM(test1) FROM foo LIMIT 1 OFFSET 20;";
        Statement select = select().sum("test1").from("foo").limit(1,20);
        assertEquals(query,select.toString());
    }

    @Test
    public void testAggregateCompination() {

        String query = "SELECT MAX(test1),MIN(test2),COUNT(test3),SUM(test4) FROM foo LIMIT 1 OFFSET 20;";
        Statement select = select().max("test1").min("test2").count("test3").sum("test4").from("foo").limit(1,20);
        assertEquals(query,select.toString());
    }

    @Test
    public void testDistinctFunction() {

        String query = "SELECT DISTINCT MAX(test1) FROM foo LIMIT 1 OFFSET 20;";
        Statement select = select().max("test1").min("test2").count("test3").sum("test4").from("foo").limit(1, 20);
        assertEquals(query, select.toString());
    }
}
