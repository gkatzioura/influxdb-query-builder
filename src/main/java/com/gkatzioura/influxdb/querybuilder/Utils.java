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
import java.util.regex.Pattern;

abstract class Utils {

    private static final Pattern cnamePattern = Pattern.compile("\\w+(?:\\[.+\\])?");


    static StringBuilder joinAndAppend(StringBuilder sb, String separator, List<? extends Appendeable> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            values.get(i).appendTo(sb);
        }
        return sb;
    }

    static StringBuilder joinAndAppendNames(StringBuilder sb, List<?> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(",");
            appendName(values.get(i), sb);
        }
        return sb;
    }

    static StringBuilder joinAndAppendValues(StringBuilder sb, List<?> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(",");
            appendValue(values.get(i), sb);
        }
        return sb;
    }

    static StringBuilder appendValue(Object value, StringBuilder sb) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof FCall) {
            FCall fcall = (FCall) value;
            sb.append(fcall.name).append('(');
            for (int i = 0; i < fcall.parameters.length; i++) {
                if (i > 0)
                    sb.append(',');
                appendValue(fcall.parameters[i], sb);
            }
            sb.append(')');
            //CName?
        } else if (value instanceof CName) {
            appendName(((CName) value).name, sb);
        } else if (value instanceof RawString) {
            sb.append(value.toString());
        } else if (value instanceof String ) {
            sb.append("'").append(value).append("'");
        } else if (value != null) {
            sb.append(value);
        } else {
            sb.append('?');
            return sb;
        }
        return sb;
    }

    static StringBuilder appendName(String name, StringBuilder sb) {
        name = name.trim();
        if (name.startsWith("\"") || name.startsWith("token(") || cnamePattern.matcher(name).matches())
            sb.append(name);
        else
            sb.append('"').append(name).append('"');
        return sb;
    }

    static StringBuilder appendName(Object name, StringBuilder sb) {
        if (name instanceof String) {
            appendName((String) name, sb);
        } else if (name instanceof CName) {
            appendName(((CName) name).name, sb);
        } else if (name instanceof Path) {
            String[] segments = ((Path) name).segments;
            for (int i = 0; i < segments.length; i++) {
                if (i > 0)
                    sb.append('.');
                appendName(segments[i], sb);
            }
        } else if (name instanceof FCall) {
            FCall fcall = (FCall) name;
            sb.append(fcall.name).append('(');
            for (int i = 0; i < fcall.parameters.length; i++) {
                if (i > 0)
                    sb.append(',');
                appendValue(fcall.parameters[i], sb);
            }
            sb.append(')');
        } else if (name instanceof Alias) {
            Alias alias = (Alias) name;
            appendName(alias.column, sb);
            sb.append(" AS ").append(alias.alias);
        } else if (name instanceof RawString) {
            sb.append(((RawString) name).str);
        } else {
            throw new IllegalArgumentException(String.format("Invalid column %s of type unknown of the query builder", name));
        }
        return sb;
    }

    static abstract class Appendeable {
        abstract void appendTo(StringBuilder sb);
    }

    static class RawString {
        private final String str;

        RawString(String str) {
            this.str = str;
        }

        @Override
        public String toString() {
            return str;
        }
    }

    static class FCall {

        private final String name;
        private final Object[] parameters;

        FCall(String name, Object... parameters) {
            this.name = name;
            this.parameters = parameters;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(name).append('(');
            for (int i = 0; i < parameters.length; i++) {
                if (i > 0)
                    sb.append(',');
                sb.append(parameters[i]);
            }
            sb.append(')');
            return sb.toString();
        }

    }

    static class CName {
        private final String name;

        CName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    static class Alias {
        private final Object column;
        private final String alias;

        Alias(Object column, String alias) {
            this.column = column;
            this.alias = alias;
        }

        @Override
        public String toString() {
            return String.format("%s AS %s", column, alias);
        }
    }


    static class Path {

        private final String[] segments;

        Path(String... segments) {
            this.segments = segments;
        }

    }

}
