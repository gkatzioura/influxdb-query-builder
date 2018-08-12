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
