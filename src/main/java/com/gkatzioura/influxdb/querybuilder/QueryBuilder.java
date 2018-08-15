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
        Appender.appendName(columnName, sb);
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
        return new RawString(str);
    }

    public static Object fcall(String name, Object... parameters) {
        return new Function(name, parameters);
    }

    public static Object now() {
        return new Function("now");
    }

    public static Object column(String name) {
        return new Column(name);
    }

    public static Object alias(Object column, String alias) {
        return new Alias(column, alias);
    }

    public static Object count(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Function("COUNT", column);
    }

    public static Object max(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Function("MAX", column);
    }

    public static Object min(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Function("MIN", column);
    }

    public static Object sum(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Function("SUM", column);
    }

    public static Object mean(Object column) {
        if (column instanceof String)
            column = column(((String) column));
        return new Function("MEAN", column);
    }

}
