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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class Select extends BuiltStatement {

    private static final List<Object> COUNT_ALL = Collections.singletonList(new Utils.FCall("count", new Utils.RawString("*")));

    private final String table;
    private final boolean isDistinct;
    private final List<Object> columns;
    private final Where where;
    private Ordering ordering;
    private List<Object> groupByColumns;
    private Integer limit;
    private Long offSet;

    Select(String database,
           String table,
           List<Object> columns,
           boolean isDistinct) {
        super(database);
        this.table = table;
        this.columns = columns;
        this.isDistinct = isDistinct;
        this.where = new Where(this);
    }

    @Override
    StringBuilder buildQueryString() {
        StringBuilder builder = new StringBuilder();

        builder.append("SELECT ");

        if (isDistinct)
            builder.append("DISTINCT ");

        if (columns == null) {
            builder.append('*');
        } else {
            Utils.joinAndAppendNames(builder, columns);
        }
        builder.append(" FROM ");

        Utils.appendName(table, builder);

        if (!where.clauses.isEmpty()) {
            builder.append(" WHERE ");
            Utils.joinAndAppend(builder, " AND ", where.clauses );
        }

        if (groupByColumns != null) {
            builder.append(" GROUP BY ");
            Utils.joinAndAppendNames(builder, groupByColumns);
        }

        if (ordering != null) {
            builder.append(" ORDER BY ");
            Utils.joinAndAppend(builder, ",", Collections.singletonList(ordering));
        }

        if (limit != null) {
            builder.append(" LIMIT ").append(limit);
        }

        if (offSet != null) {
            builder.append(" OFFSET ").append(offSet);
        }

        return builder;
    }


    public Where where(Clause clause) {
        return where.and(clause);
    }

    public Where where() {
        return where;
    }

    public Select orderBy(Ordering ordering) {

        this.ordering = ordering;
        return this;
    }

    public Select groupBy(Object... columns) {
        this.groupByColumns = Arrays.asList(columns);
        return this;
    }

    public Select limit(int limit) {
        if (limit <= 0)
            throw new IllegalArgumentException("Invalid LIMIT value, must be strictly positive");

        if (this.limit != null)
            throw new IllegalStateException("A LIMIT value has already been provided");

        this.limit = limit;
        setDirty();
        return this;
    }

    public Select limit(int limit, long offSet) {
        if (limit <= 0|| offSet<=0)
            throw new IllegalArgumentException("Invalid LIMIT and OFFSET Value, must be strictly positive");

        this.limit = limit;
        this.offSet = offSet;
        setDirty();
        return this;
    }

    public static class Where extends BuiltStatement.ForwardingStatement<Select> {

        private final List<Clause> clauses = new ArrayList<Clause>();

        Where(Select statement) {
            super(statement);
        }

        public Where and(Clause clause) {
            clauses.add(clause);
            checkForBindMarkers(clause);
            return this;
        }

        public Select orderBy(Ordering orderings) {
            return statement.orderBy(orderings);
        }

        public Select groupBy(Object... columns) {
            return statement.groupBy(columns);
        }

        public Select limit(int limit) {
            return statement.limit(limit);
        }

        public Select limit(int limit, long offSet) {
            return statement.limit(limit,offSet);
        }
    }

    public static class Builder {

        List<Object> columnNames;
        boolean isDistinct;

        Builder() {
        }

        Builder(List<Object> columnNames) {
            this.columnNames = columnNames;
        }

        public Builder distinct(String column) {

            if(columnNames!=null) {
                throw new IllegalStateException("DISTINCT function can only be used with one column");
            }

            this.columnNames = Collections.singletonList(column);
            this.isDistinct = true;
            return this;
        }

        public Select from(String table) {
            return from(null, table);
        }

        public Select from(String database, String table) {
            return new Select(database, table, columnNames, isDistinct);
        }

    }

    public static abstract class Selection extends Builder {

        @Override
        public Selection distinct(String column) {

            if(columnNames!=null) {
                throw new IllegalStateException("DISTINCT function can only be used with one column");
            }

            this.columnNames = Collections.singletonList(column);
            this.isDistinct = true;
            return this;
        }

        public abstract Builder all();

        public abstract Builder countAll();

        public abstract SelectionOrAlias column(String name);

        public abstract SelectionOrAlias fcall(String name, Object... parameters);

        public SelectionOrAlias raw(String rawString) {
            // This method should be abstract like others here. But adding an abstract method is not binary-compatible,
            // so we add this dummy implementation to make Clirr happy.
            throw new UnsupportedOperationException("Not implemented. This should only happen if you've written your own implementation of Selection");
        }

        public SelectionOrAlias count(Object column) {
            throw new UnsupportedOperationException("Not implemented. This should only happen if you've written your own implementation of Selection");
        }

        public SelectionOrAlias max(Object column) {
            throw new UnsupportedOperationException("Not implemented. This should only happen if you've written your own implementation of Selection");
        }

        public SelectionOrAlias min(Object column) {
            throw new UnsupportedOperationException("Not implemented. This should only happen if you've written your own implementation of Selection");
        }

        public SelectionOrAlias sum(Object column) {
            throw new UnsupportedOperationException("Not implemented. This should only happen if you've written your own implementation of Selection");
        }

        public SelectionOrAlias mean(Object column) {
            throw new UnsupportedOperationException("Not implemented. This should only happen if you've written your own implementation of Selection");
        }

    }

    public static class SelectionOrAlias extends Selection {

        private Object previousSelection;

        /**
         * Adds an alias for the just selected item.
         *
         * @param alias the name of the alias to use.
         * @return this in-build SELECT statement
         */
        public Selection as(String alias) {
            assert previousSelection != null;
            Object a = new Utils.Alias(previousSelection, alias);
            previousSelection = null;
            return addName(a);
        }

        // We don't return SelectionOrAlias on purpose
        private Selection addName(Object name) {
            if (columnNames == null)
                columnNames = new ArrayList<Object>();

            columnNames.add(name);
            return this;
        }

        private SelectionOrAlias queueName(Object name) {
            if (previousSelection != null)
                addName(previousSelection);

            previousSelection = name;
            return this;
        }

        @Override
        public Builder all() {
            if (isDistinct)
                throw new IllegalStateException("DISTINCT function can only be used with one column");
            if (columnNames != null)
                throw new IllegalStateException(String.format("Some columns (%s) have already been selected.", columnNames));
            if (previousSelection != null)
                throw new IllegalStateException(String.format("Some columns ([%s]) have already been selected.", previousSelection));

            return (Builder) this;
        }

        @Override
        public Builder countAll() {
            if (columnNames != null)
                throw new IllegalStateException(String.format("Some columns (%s) have already been selected.", columnNames));
            if (previousSelection != null)
                throw new IllegalStateException(String.format("Some columns ([%s]) have already been selected.", previousSelection));

            columnNames = COUNT_ALL;
            return (Builder) this;
        }

        @Override
        public SelectionOrAlias column(String name) {

            if(isDistinct) {
                throw new IllegalStateException("aggregate function distinct() cannot be combined with other functions or fields");
            }

            return queueName(name);
        }

        @Override
        public SelectionOrAlias fcall(String name, Object... parameters) {
            return queueName(new Utils.FCall(name, parameters));
        }

        @Override
        public SelectionOrAlias raw(String rawString) {
            return queueName(QueryBuilder.raw(rawString));
        }

        @Override
        public SelectionOrAlias count(Object column) {
            return queueName(QueryBuilder.count(column));
        }

        @Override
        public SelectionOrAlias max(Object column) {
            return queueName(QueryBuilder.max(column));
        }

        @Override
        public SelectionOrAlias min(Object column) {
            return queueName(QueryBuilder.min(column));
        }

        @Override
        public SelectionOrAlias sum(Object column) {
            return queueName(QueryBuilder.sum(column));
        }

        @Override
        public SelectionOrAlias mean(Object column) {
            return queueName(QueryBuilder.mean(column));
        }

        @Override
        public Select from(String keyspace, String table) {
            if (previousSelection != null)
                addName(previousSelection);
            previousSelection = null;
            return super.from(keyspace, table);
        }

        @Override
        public Select from(String table) {
            if (previousSelection != null)
                addName(previousSelection);
            previousSelection = null;
            return super.from(table);
        }
    }
}
