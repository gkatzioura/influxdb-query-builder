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
    Boolean isCounterOp;
    boolean hasBindMarkers;

    BuiltStatement(String database) {
        this.database = database;
    }

    @Override
    public String getQueryString() {
        maybeRebuildCache();
        return cache;
    }

    private void maybeRebuildCache() {
        if (!dirty && cache != null)
            return;

        StringBuilder sb = buildQueryString();
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

    abstract StringBuilder buildQueryString();

    boolean isCounterOp() {
        return isCounterOp == null ? false : isCounterOp;
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
    public String toString() {
        return maybeAddSemicolon(buildQueryString()).toString();
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
        StringBuilder buildQueryString() {
            return statement.buildQueryString();
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
