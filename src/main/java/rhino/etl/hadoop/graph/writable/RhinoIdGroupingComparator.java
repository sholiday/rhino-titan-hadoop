package rhino.etl.hadoop.graph.writable;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RhinoIdGroupingComparator extends WritableComparator {
  public RhinoIdGroupingComparator() {
    super(RhinoIdWritable.class, true);
  }

  public static int compare(long x, long y) {
    return (x < y) ? -1 : ((x == y) ? 0 : 1);
  }

  public int compare(WritableComparable k1, WritableComparable k2) {
    RhinoIdWritable r1 = (RhinoIdWritable) k1;
    RhinoIdWritable r2 = (RhinoIdWritable) k2;
    return compare(r1.rhinoId, r2.rhinoId) ;
  }
}