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

import java.util.List;


public abstract class Clause extends Utils.Appendeable {

    abstract String name();

    abstract Object firstValue();

    private static abstract class AbstractClause extends Clause {
        final String name;

        private AbstractClause(String name) {
            this.name = name;
        }

        @Override
        String name() {
            return name;
        }
    }

    static class SimpleClause extends AbstractClause {

        private final String op;
        private final Object value;

        SimpleClause(String name, String op, Object value) {
            super(name);
            this.op = op;
            this.value = value;
        }

        @Override
        void appendTo(StringBuilder sb) {
            Utils.appendName(name, sb).append(op);
            Utils.appendValue(value, sb);
        }

        @Override
        Object firstValue() {
            return value;
        }

        @Override
        boolean containsBindMarker() {
            return Utils.containsBindMarker(value);
        }
    }

    static class RegexClause extends AbstractClause {

        private final Utils.RawString value;

        RegexClause(String name, String value) {
            super(name);
            this.value = new Utils.RawString(value);

            if (value == null)
                throw new IllegalArgumentException("Missing value for regex clause");
        }

        @Override
        Object firstValue() {
            return value;
        }

        @Override void appendTo(StringBuilder sb) {
            Utils.appendName(name, sb).append(" =~ ");
            Utils.appendValue(value, sb);
        }

        @Override boolean containsBindMarker() {
            return Utils.containsBindMarker(value);
        }
    }

    static class ContainsClause extends RegexClause {

        ContainsClause(String name, String value) {
            super(name,"/*"+value+"*/");
        }
    }

    static class CompoundClause extends Clause {
        private String op;
        private final List<String> names;
        private final List<?> values;

        CompoundClause(List<String> names, String op, List<?> values) {
            this.op = op;
            this.names = names;
            this.values = values;
            if (this.names.size() != this.values.size())
                throw new IllegalArgumentException(String.format("The number of names (%d) and values (%d) don't match", this.names.size(), this.values.size()));
        }

        @Override
        String name() {
            return null;
        }

        @Override
        Object firstValue() {
            return null;
        }

        @Override
        boolean containsBindMarker() {
            for (Object value : values)
                if (Utils.containsBindMarker(value))
                    return true;
            return false;
        }

        @Override
        void appendTo(StringBuilder sb) {
            sb.append("(");
            for (int i = 0; i < names.size(); i++) {
                if (i > 0)
                    sb.append(",");
                Utils.appendName(names.get(i), sb);
            }
            sb.append(")").append(op).append("(");
            for (int i = 0; i < values.size(); i++) {
                if (i > 0)
                    sb.append(",");
                Utils.appendValue(values.get(i), sb);
            }
            sb.append(")");
        }
    }

}
