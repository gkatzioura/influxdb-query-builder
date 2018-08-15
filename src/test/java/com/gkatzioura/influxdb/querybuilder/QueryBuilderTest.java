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

import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.eq;
import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.gt;
import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.lte;
import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.select;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class QueryBuilderTest {

    @Test
    public void testSelect() {

        String query = "SELECT * FROM foo WHERE k=4 AND c>'a' AND c<='z';";

        Statement select = select().all().from("foo").where(eq("k", 4)).and(gt("c", "a")).and(lte("c", "z"));
        assertEquals(query,select.toString());

    }

    @Test
    public void testMean() {

        String query = "SELECT MEAN(k) FROM foo WHERE k=4 AND c>'a' AND c<='z';";

        Statement select = select().mean("k")
                                   .from("foo")
                                   .where(eq("k", 4))
                                   .and(gt("c", "a"))
                                   .and(lte("c", "z"));

        assertEquals(query,select.toString());
    }

    @Test
    public void testSum() {

        String query = "SELECT SUM(k) FROM foo WHERE k=4 AND c>'a' AND c<='z';";

        Statement select = select().sum("k")
                                   .from("foo")
                                   .where(eq("k", 4))
                                   .and(gt("c", "a"))
                                   .and(lte("c", "z"));


        assertEquals(query,select.toString());
    }

    @Test
    public void testMin() {

        String query = "SELECT MIN(k) FROM foo WHERE k=4 AND c>'a' AND c<='z';";

        Statement select = select().min("k")
                                   .from("foo")
                                   .where(eq("k", 4))
                                   .and(gt("c", "a"))
                                   .and(lte("c", "z"));


        assertEquals(query,select.toString());
    }

    @Test
    public void testMax() {

        String query = "SELECT MAX(k) FROM foo WHERE k=4 AND c>'a' AND c<='z';";

        Statement select = select().max("k")
                                   .from("foo")
                                   .where(eq("k", 4))
                                   .and(gt("c", "a"))
                                   .and(lte("c", "z"));


        assertEquals(query,select.toString());
    }
}
