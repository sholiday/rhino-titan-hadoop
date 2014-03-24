package rhino.etl.hadoop.graph.writable;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import org.apache.hadoop.mapreduce.Partitioner;

public class RhinoIdPartitioner extends Partitioner<RhinoIdWritable, TEdgeOrTitanIdWritable> {

  @Override
  public int getPartition(RhinoIdWritable key, TEdgeOrTitanIdWritable value, int numPartitions) {
    return (int)(Math.abs(key.rhinoId) ^ (key.rhinoId >>> 32)) % numPartitions;
   }
}