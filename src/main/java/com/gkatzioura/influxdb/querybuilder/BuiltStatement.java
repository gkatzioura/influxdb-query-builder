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
import java.util.List;
import java.util.Map;

import com.gkatzioura.influxdb.querybuilder.statement.RegularStatement;

public abstract class BuiltStatement extends RegularStatement {

    final String database;

    private boolean dirty;
    private String cache;
    private List<Object> values;
    Boolean isCounterOp;

    boolean hasBindMarkers;
    private boolean forceNoValues;

    BuiltStatement(String database) {
        this.database = database;
    }

    @Override
    public String getQueryString() {
        maybeRebuildCache();
        return cache;
    }

    public Object getObject(int i) {
        maybeRebuildCache();
        if (values == null || values.isEmpty())
            throw new IllegalStateException("This statement does not have values");
        if (i < 0 || i >= values.size())
            throw new ArrayIndexOutOfBoundsException(i);
        return values.get(i);
    }

    private void maybeRebuildCache() {
        if (!dirty && cache != null)
            return;

        StringBuilder sb;
        values = null;

        if (hasBindMarkers || forceNoValues) {
            sb = buildQueryString(null);
        } else {
            values = new ArrayList<Object>();
            sb = buildQueryString(values);

            if (values.size() > 65535)
                throw new IllegalArgumentException("Too many values for built statement, the maximum allowed is 65535");

            if (values.isEmpty())
                values = null;
        }

        maybeAddSemicolon(sb);

        cache = sb.toString();
        dirty = false;
    }

    static StringBuilder maybeAddSemicolon(StringBuilder sb) {
        // Use the same test that String#trim() uses to determine
        // if a character is a whitespace character.
        int l = sb.length();
        while (l > 0 && sb.charAt(l - 1) <= ' ')
            l -= 1;
        if (l != sb.length())
            sb.setLength(l);

        if (l == 0 || sb.charAt(l - 1) != ';')
            sb.append(';');
        return sb;
    }

    abstract StringBuilder buildQueryString(List<Object> variables);

    boolean isCounterOp() {
        return isCounterOp == null ? false : isCounterOp;
    }

    void setCounterOp(boolean isCounterOp) {
        this.isCounterOp = isCounterOp;
    }

    void setDirty() {
        dirty = true;
    }

    void checkForBindMarkers(Object value) {
        dirty = true;
        if (Utils.containsBindMarker(value))
            hasBindMarkers = true;
    }

    void checkForBindMarkers(Utils.Appendeable value) {
        dirty = true;
        if (value != null && value.containsBindMarker())
            hasBindMarkers = true;
    }

    @Override
    public String getDatabase() {
        return database;
    }

    @Override
    public Object[] getValues() {
        maybeRebuildCache();
        return values.toArray();
    }

    @Override
    public boolean hasValues() {
        maybeRebuildCache();
        return values != null;
    }

    @Override
    public Map<String, Object> getNamedValues() {

        //TODO no more return values - build statements won't return any.
        return null;
    }

    @Override
    public boolean usesNamedValues() {
        return false;
    }


    @Override
    public String toString() {
        try {
            if (forceNoValues)
                return getQueryString();
            // 1) try first with all values inlined (will not work if some values require custom codecs,
            // or if the required codecs are registered in a different CodecRegistry instance than the default one)
            return maybeAddSemicolon(buildQueryString(null)).toString();
        } catch (RuntimeException e1) {
            // 2) try next with bind markers for all values to avoid usage of custom codecs
            try {
                return maybeAddSemicolon(buildQueryString(new ArrayList<Object>())).toString();
            } catch (RuntimeException e2) {
                // Ugly but we have absolutely no context to get the registry from
                return String.format("built query (could not generate with default codec registry: %s)", e2.getMessage());
            }
        }
    }


    /**
     * An utility class to create a BuiltStatement that encapsulate another one.
     */
    abstract static class ForwardingStatement<T extends BuiltStatement> extends BuiltStatement {

        T statement;

        ForwardingStatement(T statement) {
            super(null);
            this.statement = statement;
        }

        @Override
        public String getQueryString() {
            return statement.getQueryString();
        }

        @Override
        StringBuilder buildQueryString(List<Object> values) {
            return statement.buildQueryString(values);
        }

        @Override
        public String getDatabase() {
            return statement.getDatabase();
        }

        @Override
        boolean isCounterOp() {
            return statement.isCounterOp();
        }

        @Override
        public Object[] getValues() {
            return statement.getValues();
        }

        @Override
        public boolean hasValues() {
            return statement.hasValues();
        }

        @Override
        void checkForBindMarkers(Object value) {
            statement.checkForBindMarkers(value);
        }

        @Override
        void checkForBindMarkers(Utils.Appendeable value) {
            statement.checkForBindMarkers(value);
        }

        @Override
        public String toString() {
            return statement.toString();
        }
    }
}
