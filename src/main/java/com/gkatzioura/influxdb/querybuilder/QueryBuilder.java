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

import java.util.Arrays;
import java.util.List;

import static com.gkatzioura.influxdb.querybuilder.Operations.EQ;
import static com.gkatzioura.influxdb.querybuilder.Operations.GT;
import static com.gkatzioura.influxdb.querybuilder.Operations.GTE;
import static com.gkatzioura.influxdb.querybuilder.Operations.LT;
import static com.gkatzioura.influxdb.querybuilder.Operations.LTE;
import static com.gkatzioura.influxdb.querybuilder.Operations.NE;


public final class QueryBuilder {

    private QueryBuilder() {
    }

    public static Select.Builder select(String... columns) {
        return select((Object[]) columns);
    }

    public static Select.Builder select(Object... columns) {
        return new Select.Builder(Arrays.asList(columns));
    }

    public static Select.Selection select() {
        return new Select.SelectionOrAlias();
    }

    public static String token(String columnName) {
        StringBuilder sb = new StringBuilder();
        sb.append("token(");
        Utils.appendName(columnName, sb);
        sb.append(')');
        return sb.toString();
    }

    public static Clause eq(String name, Object value) {
        return new Clause.SimpleClause(name, EQ, value);
    }

    public static Clause eq(List<String> names, List<?> values) {
        return new Clause.CompoundClause(names, EQ, values);
    }

    public static Clause ne(String name, Object value) {
        return new Clause.SimpleClause(name, NE, value);
    }

    public static Clause ne(List<String> names, List<?> values) {
        return new Clause.CompoundClause(names, NE, values);
    }

    public static Clause contains(String name, String value) {
        return new Clause.ContainsClause(name, value);
    }

    public static Clause regex(String name, String value) {
        return new Clause.RegexClause(name, value);
    }

    public static Clause lt(String name, Object value) {
        return new Clause.SimpleClause(name, LT, value);
    }

    public static Clause lt(List<String> names, List<?> values) {
        return new Clause.CompoundClause(names, LT, values);
    }

    public static Clause lte(String name, Object value) {
        return new Clause.SimpleClause(name, LTE, value);
    }

    public static Clause lte(List<String> names, List<?> values) {
        return new Clause.CompoundClause(names, LTE, values);
    }

    public static Clause gt(String name, Object value) {
        return new Clause.SimpleClause(name, GT, value);
    }

    public static Clause gt(List<String> names, List<?> values) {
        return new Clause.CompoundClause(names, GT, values);
    }

    public static Clause gte(String name, Object value) {
        return new Clause.SimpleClause(name, GTE, value);
    }

    public static Clause gte(List<String> names, List<?> values) {
        return new Clause.CompoundClause(names, GTE, values);
    }

    /**
     * @return
     */
    public static Ordering asc() {
        return new Ordering( false);
    }

    /**
     * InfluxDB supports only time for ordering
     * @return
     */
    public static Ordering desc() {
        return new Ordering( true);
    }

    public static Object raw(String str) {
        return new Utils.RawString(str);
    }

    public static Object fcall(String name, Object... parameters) {
        return new Utils.FCall(name, parameters);
    }

    public static Object now() {
        return new Utils.FCall("now");
    }

    public static Object column(String name) {
        return new Utils.CName(name);
    }

    public static Object alias(Object column, String alias) {
        return new Utils.Alias(column, alias);
    }

    public static Object count(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Utils.FCall("COUNT", column);
    }

    public static Object max(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Utils.FCall("MAX", column);
    }

    public static Object min(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Utils.FCall("MIN", column);
    }

    public static Object sum(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Utils.FCall("SUM", column);
    }

    public static Object mean(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Utils.FCall("MEAN", column);
    }

}
