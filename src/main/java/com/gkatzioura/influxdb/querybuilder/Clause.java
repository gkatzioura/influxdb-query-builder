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

import java.util.List;


public abstract class Clause implements Appendeable {

    private static abstract class AbstractClause extends Clause {
        final String name;

        private AbstractClause(String name) {
            this.name = name;
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
        public void appendTo(StringBuilder sb) {
            Appender.appendName(name, sb).append(op);
            Appender.appendValue(value, sb);
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
        public void appendTo(StringBuilder sb) {
            Appender.appendName(name, sb).append(" =~ ");
            Appender.appendValue(value, sb);
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
        public void appendTo(StringBuilder sb) {
            sb.append("(");
            for (int i = 0; i < names.size(); i++) {
                if (i > 0)
                    sb.append(",");
                Appender.appendName(names.get(i), sb);
            }
            sb.append(")").append(op).append("(");
            for (int i = 0; i < values.size(); i++) {
                if (i > 0)
                    sb.append(",");
                Appender.appendValue(values.get(i), sb);
            }
            sb.append(")");
        }
    }

}
