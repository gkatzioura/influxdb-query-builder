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

import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.asc;
import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.eq;
import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.gt;
import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.lte;
import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.select;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OrderingTest {

    @Test
    public void testOrdering() {

        String  query = "SELECT * FROM foo WHERE k=4 AND c>'a' AND c<='z' ORDER BY time ASC;";

        Select select = select().all().from("foo")
                                    .where(eq("k", 4))
                                    .and(gt("c", "a"))
                                    .and(lte("c", "z"))
                                    .orderBy(asc());
        assertEquals(query,select.toString());
    }

}
