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

import com.gkatzioura.influxdb.querybuilder.statement.Statement;

import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.eq;
import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.select;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class WhereTest {

    @Test
    public void testSelectField() {

        String query;
        Statement select;
        query = "SELECT test1,test2 FROM foo WHERE test1='testval';";
        select = select().column("test1").column("test2") .from("foo").where(eq("test1","testval"));
        assertEquals(select.toString(), query);
    }

}
