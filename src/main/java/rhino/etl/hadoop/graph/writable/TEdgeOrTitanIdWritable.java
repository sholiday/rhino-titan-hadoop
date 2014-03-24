package rhino.etl.hadoop.graph.writable;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import rhino.etl.hadoop.graph.thrift.TEdgeOrTitanId;
import rhino.etl.hadoop.thrift.ThriftWritable;

public class TEdgeOrTitanIdWritable extends ThriftWritable<TEdgeOrTitanId> {
  public TEdgeOrTitanIdWritable() {
    super(new TEdgeOrTitanId());
  }

  public TEdgeOrTitanIdWritable(final TEdgeOrTitanId p) {
    super(new TEdgeOrTitanId(p));
  }
}