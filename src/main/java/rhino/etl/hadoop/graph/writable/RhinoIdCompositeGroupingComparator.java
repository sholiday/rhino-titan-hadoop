package rhino.etl.hadoop.graph.writable;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RhinoIdCompositeGroupingComparator extends WritableComparator {
  public RhinoIdCompositeGroupingComparator() {
    super(RhinoIdWritable.class, true);
  }

  public static int compare(long x, long y) {
    return (x < y) ? -1 : ((x == y) ? 0 : 1);
  }

  public int compare(WritableComparable k1, WritableComparable k2) {
    RhinoIdWritable r1 = (RhinoIdWritable) k1;
    RhinoIdWritable r2 = (RhinoIdWritable) k2;
    return compareTo(r1, r2) ;
  }

  // If the this one is the one without an edge then it should appear first.
  public int compareTo(RhinoIdWritable a, RhinoIdWritable b) {
    int returnVal = compare(a.rhinoId, b.rhinoId);
    if(returnVal != 0){
      return returnVal;
    }
    if (!a.hasEdge) {
      return -1;
    } else if (!b.hasEdge) {
      return 1;
    } else {
      return 0;
    }
  }
}