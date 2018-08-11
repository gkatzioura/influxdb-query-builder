/*
 * Copyright 2018 Emmanouil Gkatziouras
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    public void testAggregateDistinct() {
        String query = "SELECT COUNT(DISTINCT test1) FROM foo LIMIT 1 OFFSET 20;";
        Statement select = select().count("DISTINCT test1 ").from("foo").limit(1,20);
//        assertEquals(query,select.toString());
    }

}
