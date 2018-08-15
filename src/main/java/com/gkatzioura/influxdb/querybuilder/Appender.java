package com.gkatzioura.influxdb.querybuilder;

import java.util.List;
import java.util.regex.Pattern;

public class Appender {

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

    static StringBuilder appendValue(Object value, StringBuilder sb) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof Utils.Function) {
            Utils.Function fcall = (Utils.Function) value;
            sb.append(fcall.getName()).append('(');
            for (int i = 0; i < fcall.getParameters().length; i++) {
                if (i > 0)
                    sb.append(',');
                appendValue(fcall.getParameters()[i], sb);
            }
            sb.append(')');
        } else if (value instanceof Utils.Column) {
            appendName(((Utils.Column) value).getName(), sb);
        } else if (value instanceof Utils.RawString) {
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
        } else if (name instanceof Utils.Column) {
            appendName(((Utils.Column) name).getName(), sb);
        } else if (name instanceof Utils.Path) {
            String[] segments = ((Utils.Path) name).getSegments();
            for (int i = 0; i < segments.length; i++) {
                if (i > 0)
                    sb.append('.');
                appendName(segments[i], sb);
            }
        } else if (name instanceof Utils.Function) {
            Utils.Function fcall = (Utils.Function) name;
            sb.append(fcall.getName()).append('(');
            for (int i = 0; i < fcall.getParameters().length; i++) {
                if (i > 0)
                    sb.append(',');
                appendValue(fcall.getParameters()[i], sb);
            }
            sb.append(')');
        } else if (name instanceof Utils.Alias) {
            Utils.Alias alias = (Utils.Alias) name;
            appendName(alias.getColumn(), sb);
            sb.append(" AS ").append(alias.getAlias());
        } else if (name instanceof Utils.RawString) {
            sb.append(name);
        } else {
            throw new IllegalArgumentException(String.format("Invalid column %s of type unknown of the query builder", name));
        }
        return sb;
    }
}
