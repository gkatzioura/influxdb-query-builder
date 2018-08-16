/**
 * MIT License
 * <p>
 * Copyright (c) 2018 Emmanouil Gkatziouras
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * <p>
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
        Statement statement = select().column("test1").distinct().column("test2").from("foo");
        assertThrows(IllegalStateException.class, () -> statement.toString(), "a message");
    }

}
