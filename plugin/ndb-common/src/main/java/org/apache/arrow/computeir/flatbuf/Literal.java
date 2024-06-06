// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.computeir.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class Literal extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static Literal getRootAsLiteral(ByteBuffer _bb) { return getRootAsLiteral(_bb, new Literal()); }
  public static Literal getRootAsLiteral(ByteBuffer _bb, Literal obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Literal __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public byte implType() { int o = __offset(4); return o != 0 ? bb.get(o + bb_pos) : 0; }
  /**
   * Literal value data; for null literals do not include this field.
   */
  public Table impl(Table obj) { int o = __offset(6); return o != 0 ? __union(obj, o + bb_pos) : null; }
  /**
   * Type of the literal value. This must match `impl`.
   */
  public org.apache.arrow.flatbuf.Field type() { return type(new org.apache.arrow.flatbuf.Field()); }
  public org.apache.arrow.flatbuf.Field type(org.apache.arrow.flatbuf.Field obj) { int o = __offset(8); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }

  public static int createLiteral(FlatBufferBuilder builder,
      byte impl_type,
      int implOffset,
      int typeOffset) {
    builder.startTable(3);
    Literal.addType(builder, typeOffset);
    Literal.addImpl(builder, implOffset);
    Literal.addImplType(builder, impl_type);
    return Literal.endLiteral(builder);
  }

  public static void startLiteral(FlatBufferBuilder builder) { builder.startTable(3); }
  public static void addImplType(FlatBufferBuilder builder, byte implType) { builder.addByte(0, implType, 0); }
  public static void addImpl(FlatBufferBuilder builder, int implOffset) { builder.addOffset(1, implOffset, 0); }
  public static void addType(FlatBufferBuilder builder, int typeOffset) { builder.addOffset(2, typeOffset, 0); }
  public static int endLiteral(FlatBufferBuilder builder) {
    int o = builder.endTable();
    builder.required(o, 8);  // type
    return o;
  }
  public static void finishLiteralBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedLiteralBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Literal get(int j) { return get(new Literal(), j); }
    public Literal get(Literal obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}
