package rhino.etl.hadoop.thrift;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import org.apache.hadoop.io.Writable;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.*;

public abstract class ThriftWritable<T extends TBase> implements Writable {

  T tBase;

  public ThriftWritable() {
    tBase.clear();
  }

  public ThriftWritable(final T tBase) {
    this.tBase = tBase;
  }

  public T get() {
    return tBase;
  }

  public void set(T tBase) {
    this.tBase = tBase;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    TIOStreamTransport trans = new TIOStreamTransport(baos);
    TBinaryProtocol oprot = new TBinaryProtocol(trans);
    try {
      tBase.write(oprot);
      byte[] array = baos.toByteArray();
      out.writeInt(array.length);
      out.write(array);
    } catch (TException e) {
      throw new IOException(e.toString());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    byte[] array = new byte[length];
    in.readFully(array);
    ByteArrayInputStream bais = new ByteArrayInputStream(array);
    TIOStreamTransport trans = new TIOStreamTransport(bais);
    TBinaryProtocol oprot = new TBinaryProtocol(trans);
    try {
      tBase.clear();
      tBase.read(oprot);
    } catch (TException e) {
      throw new IOException(e.toString());
    }
  }
}
