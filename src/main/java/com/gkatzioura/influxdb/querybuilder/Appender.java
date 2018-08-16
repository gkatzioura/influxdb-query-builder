/**
 * MIT License
 * <p>
 * Copyright (c) 2018 Emmanouil Gkatziouras
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * <p>
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

public class Appender {

    private static final Pattern cnamePattern = Pattern.compile("\\w+(?:\\[.+\\])?");

    static StringBuilder joinAndAppend(StringBuilder sb, String separator, List<? extends Appendable> values) {
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

    static StringBuilder appendValue(Object value, StringBuilder sb) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof Function) {
            Function fcall = (Function) value;
            sb.append(fcall.getName()).append('(');
            for (int i = 0; i < fcall.getParameters().length; i++) {
                if (i > 0)
                    sb.append(',');
                appendValue(fcall.getParameters()[i], sb);
            }
            sb.append(')');
        } else if (value instanceof Column) {
            appendName(((Column) value).getName(), sb);
        } else if (value instanceof RawString) {
            sb.append(value.toString());
        } else if (value instanceof String) {
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
        } else if (name instanceof Column) {
            appendName(((Column) name).getName(), sb);
        } else if (name instanceof Function) {
            Function functionCall = (Function) name;
            sb.append(functionCall.getName()).append('(');
            for (int i = 0; i < functionCall.getParameters().length; i++) {
                if (i > 0)
                    sb.append(',');
                appendValue(functionCall.getParameters()[i], sb);
            }
            sb.append(')');
        } else if (name instanceof Alias) {
            Alias alias = (Alias) name;
            appendName(alias.getColumn(), sb);
            sb.append(" AS ").append(alias.getAlias());
        } else if (name instanceof RawString) {
            sb.append(name);
        } else if (name instanceof Distinct) {
            Distinct distinct = (Distinct) name;
            sb.append("DISTINCT ");
            appendName(distinct.getExpression(), sb);
        } else {
            throw new IllegalArgumentException(String.format("Invalid column %s of type unknown of the query builder", name));
        }
        return sb;
    }
}
