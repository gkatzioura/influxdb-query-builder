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

import java.util.Collections;

import org.junit.jupiter.api.Test;

import static com.gkatzioura.influxdb.querybuilder.QueryBuilder.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class UtilsTest {

    @Test
    public void testJoinAndAppend() {

        StringBuilder builder = new StringBuilder();
        builder.append("SELECT test1,test2 FROM foo WHERE ");

        Utils.joinAndAppend(builder, " AND ", Collections.singletonList(eq("testval", "test1")));

        assertEquals("SELECT test1,test2 FROM foo WHERE testval='test1'",builder.toString());
    }

}
