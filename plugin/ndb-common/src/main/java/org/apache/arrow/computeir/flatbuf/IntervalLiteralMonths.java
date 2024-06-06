// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.computeir.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class IntervalLiteralMonths extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static IntervalLiteralMonths getRootAsIntervalLiteralMonths(ByteBuffer _bb) { return getRootAsIntervalLiteralMonths(_bb, new IntervalLiteralMonths()); }
  public static IntervalLiteralMonths getRootAsIntervalLiteralMonths(ByteBuffer _bb, IntervalLiteralMonths obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public IntervalLiteralMonths __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public int months() { int o = __offset(4); return o != 0 ? bb.getInt(o + bb_pos) : 0; }

  public static int createIntervalLiteralMonths(FlatBufferBuilder builder,
      int months) {
    builder.startTable(1);
    IntervalLiteralMonths.addMonths(builder, months);
    return IntervalLiteralMonths.endIntervalLiteralMonths(builder);
  }

  public static void startIntervalLiteralMonths(FlatBufferBuilder builder) { builder.startTable(1); }
  public static void addMonths(FlatBufferBuilder builder, int months) { builder.addInt(0, months, 0); }
  public static int endIntervalLiteralMonths(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public IntervalLiteralMonths get(int j) { return get(new IntervalLiteralMonths(), j); }
    public IntervalLiteralMonths get(IntervalLiteralMonths obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}
