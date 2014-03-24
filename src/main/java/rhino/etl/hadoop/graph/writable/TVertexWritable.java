package rhino.etl.hadoop.graph.writable;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import org.apache.hadoop.io.Writable;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import rhino.etl.hadoop.graph.thrift.TVertex;

import java.io.*;

public class TVertexWritable implements Writable {
  TVertex v;
  public TVertexWritable() {
    v = new TVertex();
  }

  public TVertexWritable(final TVertex p) {
    v = p.deepCopy();
  }

  public TVertex get() {
    return v.deepCopy();
  }


  public void write(DataOutput out) throws IOException {
    //System.out.println("\twrite(" + v.toString() + ")");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    TIOStreamTransport trans = new TIOStreamTransport(baos);
    TBinaryProtocol oprot = new TBinaryProtocol(trans);
    try {
      v.write(oprot);
      byte[] array = baos.toByteArray();
      out.writeInt(array.length);
      out.write(array);
    } catch (TException e) {
      throw new IOException(e.toString());
    }
  }

  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    byte[] array = new byte[length];
    in.readFully(array);
    ByteArrayInputStream bais = new ByteArrayInputStream(array);
    TIOStreamTransport trans = new TIOStreamTransport(bais);
    TBinaryProtocol oprot = new TBinaryProtocol(trans);
    try {
      v.clear();
      v.read(oprot);
      //System.out.println("\tread(" + v.toString() + ")");
    } catch (TException e) {
      throw new IOException(e.toString());
    }
  }
}
