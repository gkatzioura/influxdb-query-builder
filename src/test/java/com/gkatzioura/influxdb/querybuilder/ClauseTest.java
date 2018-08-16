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

import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.contains;
import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.nregex;
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
    public void testNegativeRegex() {
        String query = "SELECT MAX(k) FROM foo WHERE k ~! /[0-9]/;";
        Statement select = select().max("k").from("foo").where(nregex("k", "/[0-9]/"));
        assertEquals(query, select.toString());
    }

    @Test
    public void testContains() {
        String query = "SELECT MAX(k) FROM foo WHERE k =~ /*text*/;";
        Statement select = select().max("k").from("foo").where(contains("k", "text"));
        assertEquals(query,select.toString());
    }

}
