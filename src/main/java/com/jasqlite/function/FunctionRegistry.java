package com.jasqlite.function;

import com.jasqlite.util.SQLiteValue;
import java.util.*;

/**
 * Registry of built-in SQL functions.
 * Implements SQLite's built-in scalar and aggregate functions.
 */
public class FunctionRegistry {
    
    private final Map<String, SQLFunction> functions;
    
    public FunctionRegistry() {
        this.functions = new HashMap<>();
        registerBuiltins();
    }
    
    private void registerBuiltins() {
        // Aggregate functions
        register("count", new CountFunction());
        register("sum", new SumFunction());
        register("total", new TotalFunction());
        register("avg", new AvgFunction());
        register("min", new MinFunction());
        register("max", new MaxFunction());
        register("group_concat", new GroupConcatFunction());
        
        // Scalar functions - strings
        register("length", new LengthFunction());
        register("lower", new LowerFunction());
        register("upper", new UpperFunction());
        register("substr", new SubstrFunction());
        register("substring", new SubstrFunction());
        register("replace", new ReplaceFunction());
        register("trim", new TrimFunction());
        register("ltrim", new LTrimFunction());
        register("rtrim", new RTrimFunction());
        register("ltrim", new LTrimFunction());
        register("rtrim", new RTrimFunction());
        register("instr", new InstrFunction());
        register("printf", new PrintfFunction());
        register("char", new CharFunction());
        register("hex", new HexFunction());
        register("unhex", new UnhexFunction());
        register("unicode", new UnicodeFunction());
        register("quote", new QuoteFunction());
        register("soundex", new SoundexFunction());
        register("zeroblob", new ZeroBlobFunction());
        register("typeof", new TypeofFunction());
        
        // Scalar functions - math
        register("abs", new AbsFunction());
        register("round", new RoundFunction());
        register("random", new RandomFunction());
        register("randomblob", new RandomBlobFunction());
        register("ceil", new CeilFunction());
        register("ceiling", new CeilFunction());
        register("floor", new FloorFunction());
        register("log", new LogFunction());
        register("log2", new Log2Function());
        register("ln", new LnFunction());
        register("exp", new ExpFunction());
        register("power", new PowerFunction());
        register("pow", new PowerFunction());
        register("sqrt", new SqrtFunction());
        register("sign", new SignFunction());
        register("mod", new ModFunction());
        register("trunc", new TruncFunction());
        register("pi", new PiFunction());
        register("acos", new AcosFunction());
        register("asin", new AsinFunction());
        register("atan", new AtanFunction());
        register("atan2", new Atan2Function());
        register("cos", new CosFunction());
        register("sin", new SinFunction());
        register("tan", new TanFunction());
        register("degrees", new DegreesFunction());
        register("radians", new RadiansFunction());
        
        // Scalar functions - date/time
        register("date", new DateFunction());
        register("time", new TimeFunction());
        register("datetime", new DateTimeFunction());
        register("julianday", new JulianDayFunction());
        register("strftime", new StrftimeFunction());
        register("unixepoch", new UnixEpochFunction());
        
        // Scalar functions - type conversion
        register("cast", new CastFunction());
        register("coalesce", new CoalesceFunction());
        register("ifnull", new IfNullFunction());
        register("iif", new IifFunction());
        register("nullif", new NullIfFunction());
        register("likely", new LikelyFunction());
        register("unlikely", new UnlikelyFunction());
        register("iif", new IifFunction());
        
        // Scalar functions - misc
        register("changes", new ChangesFunction());
        register("last_insert_rowid", new LastInsertRowidFunction());
        register("row_number", new RowNumberFunction());
        register("rank", new RankFunction());
        register("dense_rank", new DenseRankFunction());
        register("ntile", new NtileFunction());
        register("lead", new LeadFunction());
        register("lag", new LagFunction());
        register("first_value", new FirstValueFunction());
        register("last_value", new LastValueFunction());
        register("nth_value", new NthValueFunction());
        
        // Window aggregate
        register("count", new CountFunction());
        register("sum", new SumFunction());
        register("avg", new AvgFunction());
        register("min", new MinFunction());
        register("max", new MaxFunction());
        register("total", new TotalFunction());
        register("group_concat", new GroupConcatFunction());
    }
    
    public void register(String name, SQLFunction function) {
        functions.put(name.toLowerCase(), function);
    }
    
    public SQLFunction getFunction(String name) {
        return functions.get(name.toLowerCase());
    }
    
    public boolean hasFunction(String name) {
        return functions.containsKey(name.toLowerCase());
    }
    
    /**
     * Call a scalar function.
     */
    public SQLiteValue call(String name, SQLiteValue[] args) {
        SQLFunction fn = getFunction(name);
        if (fn == null) {
            throw new RuntimeException("no such function: " + name);
        }
        if (fn.isAggregate()) {
            throw new RuntimeException("misuse of aggregate function: " + name);
        }
        return fn.call(args);
    }
    
    // ==================== Function Interfaces ====================
    
    public interface SQLFunction {
        boolean isAggregate();
        SQLiteValue call(SQLiteValue[] args);
        AggregateState newAggregateState();
    }
    
    public static abstract class ScalarFunction implements SQLFunction {
        @Override public boolean isAggregate() { return false; }
        @Override public AggregateState newAggregateState() { return null; }
    }
    
    public static abstract class AggregateFunction implements SQLFunction {
        @Override public boolean isAggregate() { return true; }
        @Override public SQLiteValue call(SQLiteValue[] args) {
            throw new UnsupportedOperationException("Use aggregate step/finalize");
        }
    }
    
    public static class AggregateState {
        public long count;
        public double sum;
        public SQLiteValue min, max;
        public StringBuilder concat;
        public SQLiteValue value;
        
        public AggregateState() {
            this.count = 0;
            this.sum = 0;
            this.min = null;
            this.max = null;
            this.concat = new StringBuilder();
        }
    }
    
    // ==================== Aggregate Functions ====================
    
    static class CountFunction extends AggregateFunction {
        @Override public AggregateState newAggregateState() { return new AggregateState(); }
        
        public SQLiteValue step(AggregateState state, SQLiteValue[] args) {
            if (args.length == 0) {
                state.count++;
            } else if (!args[0].isNull()) {
                state.count++;
            }
            return null;
        }
        
        public SQLiteValue finalize(AggregateState state) {
            return SQLiteValue.fromLong(state.count);
        }
    }
    
    static class SumFunction extends AggregateFunction {
        @Override public AggregateState newAggregateState() { return new AggregateState(); }
        
        public SQLiteValue step(AggregateState state, SQLiteValue[] args) {
            if (args.length > 0 && !args[0].isNull()) {
                state.sum += args[0].asDouble();
                state.count++;
            }
            return null;
        }
        
        public SQLiteValue finalize(AggregateState state) {
            if (state.count == 0) return SQLiteValue.fromNull();
            double sum = state.sum;
            if (sum == Math.floor(sum) && !Double.isInfinite(sum)) {
                return SQLiteValue.fromLong((long) sum);
            }
            return SQLiteValue.fromDouble(sum);
        }
    }
    
    static class TotalFunction extends AggregateFunction {
        @Override public AggregateState newAggregateState() { return new AggregateState(); }
        
        public SQLiteValue step(AggregateState state, SQLiteValue[] args) {
            if (args.length > 0 && !args[0].isNull()) {
                state.sum += args[0].asDouble();
                state.count++;
            }
            return null;
        }
        
        public SQLiteValue finalize(AggregateState state) {
            return SQLiteValue.fromDouble(state.sum);
        }
    }
    
    static class AvgFunction extends AggregateFunction {
        @Override public AggregateState newAggregateState() { return new AggregateState(); }
        
        public SQLiteValue step(AggregateState state, SQLiteValue[] args) {
            if (args.length > 0 && !args[0].isNull()) {
                state.sum += args[0].asDouble();
                state.count++;
            }
            return null;
        }
        
        public SQLiteValue finalize(AggregateState state) {
            if (state.count == 0) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(state.sum / state.count);
        }
    }
    
    static class MinFunction extends AggregateFunction {
        @Override public AggregateState newAggregateState() { return new AggregateState(); }
        
        public SQLiteValue step(AggregateState state, SQLiteValue[] args) {
            if (args.length > 0 && !args[0].isNull()) {
                if (state.min == null || args[0].compareTo(state.min) < 0) {
                    state.min = args[0];
                }
                state.count++;
            }
            return null;
        }
        
        public SQLiteValue finalize(AggregateState state) {
            return state.min != null ? state.min : SQLiteValue.fromNull();
        }
    }
    
    static class MaxFunction extends AggregateFunction {
        @Override public AggregateState newAggregateState() { return new AggregateState(); }
        
        public SQLiteValue step(AggregateState state, SQLiteValue[] args) {
            if (args.length > 0 && !args[0].isNull()) {
                if (state.max == null || args[0].compareTo(state.max) > 0) {
                    state.max = args[0];
                }
                state.count++;
            }
            return null;
        }
        
        public SQLiteValue finalize(AggregateState state) {
            return state.max != null ? state.max : SQLiteValue.fromNull();
        }
    }
    
    static class GroupConcatFunction extends AggregateFunction {
        @Override public AggregateState newAggregateState() { return new AggregateState(); }
        
        public SQLiteValue step(AggregateState state, SQLiteValue[] args) {
            if (args.length > 0 && !args[0].isNull()) {
                if (state.concat.length() > 0) {
                    String sep = args.length > 1 ? args[1].asString() : ",";
                    state.concat.append(sep);
                }
                state.concat.append(args[0].asString());
                state.count++;
            }
            return null;
        }
        
        public SQLiteValue finalize(AggregateState state) {
            return state.count > 0 ? SQLiteValue.fromText(state.concat.toString()) : SQLiteValue.fromNull();
        }
    }
    
    // ==================== Scalar Functions - Strings ====================
    
    static class LengthFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            if (args[0].isBlob()) return SQLiteValue.fromLong(args[0].getBlob().length);
            return SQLiteValue.fromLong(args[0].asString().length());
        }
    }
    
    static class LowerFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromText(args[0].asString().toLowerCase());
        }
    }
    
    static class UpperFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromText(args[0].asString().toUpperCase());
        }
    }
    
    static class SubstrFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length < 2 || args[0].isNull()) return SQLiteValue.fromNull();
            String str = args[0].asString();
            int start = (int) args[1].asLong() - 1; // 1-based
            if (start < 0) start = 0;
            int len = args.length > 2 ? (int) args[2].asLong() : str.length() - start;
            if (len < 0) len = 0;
            int end = Math.min(start + len, str.length());
            if (start >= str.length()) return SQLiteValue.fromText("");
            return SQLiteValue.fromText(str.substring(start, end));
        }
    }
    
    static class ReplaceFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length < 3 || args[0].isNull()) return args[0];
            String str = args[0].asString();
            String find = args[1].asString();
            String replace = args[2].asString();
            return SQLiteValue.fromText(str.replace(find, replace));
        }
    }
    
    static class TrimFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            String str = args[0].asString();
            return SQLiteValue.fromText(str.trim());
        }
    }
    
    static class LTrimFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            String str = args[0].asString();
            int i = 0;
            while (i < str.length() && Character.isWhitespace(str.charAt(i))) i++;
            return SQLiteValue.fromText(str.substring(i));
        }
    }
    
    static class RTrimFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            String str = args[0].asString();
            int end = str.length();
            while (end > 0 && Character.isWhitespace(str.charAt(end - 1))) end--;
            return SQLiteValue.fromText(str.substring(0, end));
        }
    }
    
    static class InstrFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length < 2 || args[0].isNull() || args[1].isNull()) return SQLiteValue.fromNull();
            int idx = args[0].asString().indexOf(args[1].asString());
            return SQLiteValue.fromLong(idx >= 0 ? idx + 1 : 0);
        }
    }
    
    static class PrintfFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0) return SQLiteValue.fromText("");
            String fmt = args[0].asString();
            Object[] fmtArgs = new Object[args.length - 1];
            for (int i = 1; i < args.length; i++) {
                fmtArgs[i - 1] = args[i].asObject();
            }
            try {
                return SQLiteValue.fromText(String.format(fmt, fmtArgs));
            } catch (Exception e) {
                return SQLiteValue.fromText(fmt);
            }
        }
    }
    
    static class CharFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            StringBuilder sb = new StringBuilder();
            for (SQLiteValue arg : args) {
                sb.append((char) arg.asLong());
            }
            return SQLiteValue.fromText(sb.toString());
        }
    }
    
    static class HexFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            if (args[0].isBlob()) {
                byte[] blob = args[0].getBlob();
                StringBuilder sb = new StringBuilder();
                for (byte b : blob) sb.append(String.format("%02x", b & 0xFF));
                return SQLiteValue.fromText(sb.toString().toUpperCase());
            }
            return SQLiteValue.fromText(args[0].asString().toUpperCase());
        }
    }
    
    static class UnhexFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            String hex = args[0].asString();
            byte[] bytes = new byte[hex.length() / 2];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
            }
            return SQLiteValue.fromBlob(bytes);
        }
    }
    
    static class UnicodeFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            String str = args[0].asString();
            if (str.isEmpty()) return SQLiteValue.fromLong(0);
            return SQLiteValue.fromLong(str.charAt(0));
        }
    }
    
    static class QuoteFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromText("NULL");
            if (args[0].isText()) return SQLiteValue.fromText("'" + args[0].asString().replace("'", "''") + "'");
            return SQLiteValue.fromText(args[0].toString());
        }
    }
    
    static class SoundexFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromText("?000");
            String s = args[0].asString().toUpperCase();
            if (s.isEmpty()) return SQLiteValue.fromText("?000");
            char[] codes = {'0', '1', '2', '3', '0', '1', '2', '0', '0', '2', '2', '4', '5', '5', '0', '1', '2', '6', '2', '3', '0', '1', '0', '2', '0', '2'};
            StringBuilder result = new StringBuilder();
            result.append(s.charAt(0));
            char lastCode = codes[s.charAt(0) - 'A'];
            for (int i = 1; i < s.length() && result.length() < 4; i++) {
                if (s.charAt(i) >= 'A' && s.charAt(i) <= 'Z') {
                    char code = codes[s.charAt(i) - 'A'];
                    if (code != '0' && code != lastCode) {
                        result.append(code);
                    }
                    lastCode = code;
                }
            }
            while (result.length() < 4) result.append('0');
            return SQLiteValue.fromText(result.toString());
        }
    }
    
    static class ZeroBlobFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0) return SQLiteValue.fromBlob(new byte[0]);
            int size = (int) args[0].asLong();
            return SQLiteValue.fromBlob(new byte[Math.max(0, size)]);
        }
    }
    
    static class TypeofFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0) return SQLiteValue.fromText("null");
            switch (args[0].getType()) {
                case NULL: return SQLiteValue.fromText("null");
                case INTEGER: return SQLiteValue.fromText("integer");
                case FLOAT: return SQLiteValue.fromText("real");
                case TEXT: return SQLiteValue.fromText("text");
                case BLOB: return SQLiteValue.fromText("blob");
                default: return SQLiteValue.fromText("null");
            }
        }
    }
    
    // ==================== Scalar Functions - Math ====================
    
    static class AbsFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            if (args[0].isInteger()) return SQLiteValue.fromLong(Math.abs(args[0].asLong()));
            return SQLiteValue.fromDouble(Math.abs(args[0].asDouble()));
        }
    }
    
    static class RoundFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            int precision = args.length > 1 ? (int) args[1].asLong() : 0;
            double val = args[0].asDouble();
            double factor = Math.pow(10, precision);
            return SQLiteValue.fromDouble(Math.round(val * factor) / factor);
        }
    }
    
    static class RandomFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return SQLiteValue.fromLong(new java.util.Random().nextLong());
        }
    }
    
    static class RandomBlobFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            int size = args.length > 0 ? (int) args[0].asLong() : 1;
            byte[] blob = new byte[Math.max(1, size)];
            new java.util.Random().nextBytes(blob);
            return SQLiteValue.fromBlob(blob);
        }
    }
    
    static class CeilFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.ceil(args[0].asDouble()));
        }
    }
    
    static class FloorFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.floor(args[0].asDouble()));
        }
    }
    
    static class LogFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            if (args.length == 1) return SQLiteValue.fromDouble(Math.log10(args[0].asDouble()));
            return SQLiteValue.fromDouble(Math.log(args[1].asDouble()) / Math.log(args[0].asDouble()));
        }
    }
    
    static class Log2Function extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.log(args[0].asDouble()) / Math.log(2));
        }
    }
    
    static class LnFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.log(args[0].asDouble()));
        }
    }
    
    static class ExpFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.exp(args[0].asDouble()));
        }
    }
    
    static class PowerFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length < 2 || args[0].isNull() || args[1].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.pow(args[0].asDouble(), args[1].asDouble()));
        }
    }
    
    static class SqrtFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.sqrt(args[0].asDouble()));
        }
    }
    
    static class SignFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            double v = args[0].asDouble();
            return SQLiteValue.fromLong(v > 0 ? 1 : (v < 0 ? -1 : 0));
        }
    }
    
    static class ModFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length < 2) return SQLiteValue.fromNull();
            double x = args[0].asDouble(), y = args[1].asDouble();
            if (y == 0) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(x - y * Math.floor(x / y));
        }
    }
    
    static class TruncFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            double v = args[0].asDouble();
            return SQLiteValue.fromDouble(v < 0 ? Math.ceil(v) : Math.floor(v));
        }
    }
    
    static class PiFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return SQLiteValue.fromDouble(Math.PI);
        }
    }
    
    static class AcosFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.acos(args[0].asDouble()));
        }
    }
    static class AsinFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.asin(args[0].asDouble()));
        }
    }
    static class AtanFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.atan(args[0].asDouble()));
        }
    }
    static class Atan2Function extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length < 2) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.atan2(args[0].asDouble(), args[1].asDouble()));
        }
    }
    static class CosFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.cos(args[0].asDouble()));
        }
    }
    static class SinFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.sin(args[0].asDouble()));
        }
    }
    static class TanFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.tan(args[0].asDouble()));
        }
    }
    static class DegreesFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.toDegrees(args[0].asDouble()));
        }
    }
    static class RadiansFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0 || args[0].isNull()) return SQLiteValue.fromNull();
            return SQLiteValue.fromDouble(Math.toRadians(args[0].asDouble()));
        }
    }
    
    // ==================== Date/Time Functions ====================
    
    static class DateFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0) return SQLiteValue.fromText(java.time.LocalDate.now().toString());
            String modifier = args[0].asString();
            try {
                if (modifier.equals("now") || modifier.equals("'now'")) {
                    return SQLiteValue.fromText(java.time.LocalDate.now().toString());
                }
                return SQLiteValue.fromText(java.time.LocalDate.parse(modifier.substring(0, 10)).toString());
            } catch (Exception e) {
                return SQLiteValue.fromText(java.time.LocalDate.now().toString());
            }
        }
    }
    
    static class TimeFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0) return SQLiteValue.fromText(java.time.LocalTime.now().toString().substring(0, 8));
            return SQLiteValue.fromText(java.time.LocalTime.now().toString().substring(0, 8));
        }
    }
    
    static class DateTimeFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0) return SQLiteValue.fromText(java.time.LocalDateTime.now().toString().replace('T', ' ').substring(0, 19));
            String val = args[0].asString();
            if (val.equals("now") || val.equals("'now'")) {
                return SQLiteValue.fromText(java.time.LocalDateTime.now().toString().replace('T', ' ').substring(0, 19));
            }
            try {
                return SQLiteValue.fromText(val);
            } catch (Exception e) {
                return SQLiteValue.fromText(java.time.LocalDateTime.now().toString().replace('T', ' ').substring(0, 19));
            }
        }
    }
    
    static class JulianDayFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            // Simplified Julian day calculation
            return SQLiteValue.fromDouble(2460000.0 + System.currentTimeMillis() / 86400000.0);
        }
    }
    
    static class StrftimeFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length == 0) return SQLiteValue.fromText("");
            String format = args[0].asString();
            java.time.LocalDateTime now = java.time.LocalDateTime.now();
            String result = format
                .replace("%Y", String.format("%04d", now.getYear()))
                .replace("%m", String.format("%02d", now.getMonthValue()))
                .replace("%d", String.format("%02d", now.getDayOfMonth()))
                .replace("%H", String.format("%02d", now.getHour()))
                .replace("%M", String.format("%02d", now.getMinute()))
                .replace("%S", String.format("%02d", now.getSecond()))
                .replace("%s", String.valueOf(System.currentTimeMillis() / 1000))
                .replace("%J", String.valueOf(2460000.0 + System.currentTimeMillis() / 86400000.0))
                .replace("%%", "%");
            return SQLiteValue.fromText(result);
        }
    }
    
    static class UnixEpochFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return SQLiteValue.fromLong(System.currentTimeMillis() / 1000);
        }
    }
    
    // ==================== Type Conversion Functions ====================
    
    static class CastFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return args.length > 0 ? args[0] : SQLiteValue.fromNull();
        }
    }
    
    static class CoalesceFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            for (SQLiteValue arg : args) {
                if (arg != null && !arg.isNull()) return arg;
            }
            return SQLiteValue.fromNull();
        }
    }
    
    static class IfNullFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length < 2) return SQLiteValue.fromNull();
            return args[0].isNull() ? args[1] : args[0];
        }
    }
    
    static class IifFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length < 3) return SQLiteValue.fromNull();
            return args[0].asLong() != 0 ? args[1] : args[2];
        }
    }
    
    static class NullIfFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            if (args.length < 2) return SQLiteValue.fromNull();
            return args[0].equals(args[1]) ? SQLiteValue.fromNull() : args[0];
        }
    }
    
    static class LikelyFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return args.length > 0 ? args[0] : SQLiteValue.fromNull();
        }
    }
    
    static class UnlikelyFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return args.length > 0 ? args[0] : SQLiteValue.fromNull();
        }
    }
    
    // ==================== Misc Functions ====================
    
    static class ChangesFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            // Will be overridden by executor context
            return SQLiteValue.fromLong(0);
        }
    }
    
    static class LastInsertRowidFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return SQLiteValue.fromLong(0);
        }
    }
    
    static class RowNumberFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return SQLiteValue.fromLong(0);
        }
    }
    
    static class RankFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return SQLiteValue.fromLong(0);
        }
    }
    
    static class DenseRankFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return SQLiteValue.fromLong(0);
        }
    }
    
    static class NtileFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return SQLiteValue.fromLong(0);
        }
    }
    
    static class LeadFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return SQLiteValue.fromNull();
        }
    }
    
    static class LagFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return SQLiteValue.fromNull();
        }
    }
    
    static class FirstValueFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return args.length > 0 ? args[0] : SQLiteValue.fromNull();
        }
    }
    
    static class LastValueFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return args.length > 0 ? args[0] : SQLiteValue.fromNull();
        }
    }
    
    static class NthValueFunction extends ScalarFunction {
        @Override public SQLiteValue call(SQLiteValue[] args) {
            return args.length > 0 ? args[0] : SQLiteValue.fromNull();
        }
    }
}
