package rhino.etl.hadoop.graph.writable;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RhinoIdWritable implements WritableComparable<RhinoIdWritable> {
  public long rhinoId;
  public boolean hasEdge;

  public RhinoIdWritable() {

  }
  public RhinoIdWritable(long rhinoId, boolean hasEdge) {
    this.rhinoId = rhinoId;
    this.hasEdge = hasEdge;
  }

  // If the this one is the one without an edge then it should appear first.
  @Override
  public int compareTo(RhinoIdWritable other) {
    int returnVal = compare(rhinoId, other.rhinoId);
    if(returnVal != 0){
      return returnVal;
    }
    if (!hasEdge) {
      return -1;
    } else {
      return 0;
    }
  }

  public static int compare(long x, long y) {
    return (x < y) ? -1 : ((x == y) ? 0 : 1);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(rhinoId);
    out.writeBoolean(hasEdge);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    rhinoId = in.readLong();
    hasEdge = in.readBoolean();
  }
}
