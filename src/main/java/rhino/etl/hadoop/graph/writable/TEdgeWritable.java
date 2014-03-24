package rhino.etl.hadoop.graph.writable;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import rhino.etl.hadoop.graph.thrift.TEdge;
import rhino.etl.hadoop.thrift.ThriftWritable;

public class TEdgeWritable extends ThriftWritable<TEdge> {
  public TEdgeWritable() {
    super(new TEdge());
  }

  public TEdgeWritable(final TEdge p) {
    super(new TEdge(p));
  }
}
