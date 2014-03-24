package rhino.etl.hadoop.graph.insert;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import rhino.etl.hadoop.graph.thrift.TEdge;
import rhino.etl.hadoop.graph.writable.RhinoIdWritable;
import rhino.etl.hadoop.graph.writable.TEdgeOrTitanIdWritable;
import rhino.etl.hadoop.graph.writable.TEdgeWritable;

import java.io.IOException;

public class VertexInsertReducer
    extends Reducer<RhinoIdWritable, TEdgeOrTitanIdWritable, LongWritable, TEdgeWritable> {

  public enum Counters {
    NO_RIGHT_TITAN_ID_IN_REDUCE,
    NO_TITAN_ID_FOUND_IN_TIME,
    TITAN_ID_CAME_LATE,
    VERTEX_WITH_DROPPED_EDGES,
    DROPPED_EDGES
  }

  protected long edgeCutoff = -1;

  @Override
  public void setup(final Reducer.Context context) throws IOException, InterruptedException {
    edgeCutoff = context.getConfiguration().getLong("edgeCutoff", -1);
  }

  @Override
  public void reduce(
      final RhinoIdWritable key,
      final Iterable<TEdgeOrTitanIdWritable> values,
      final Reducer<RhinoIdWritable, TEdgeOrTitanIdWritable, LongWritable,
          TEdgeWritable>.Context context)
      throws IOException, InterruptedException {

    //StringBuilder sb = new StringBuilder();
    boolean tooBig = false;
    boolean willPrint = false;
    Long rightHandTitanId = null;

    int numRecords = 0;
    for (final TEdgeOrTitanIdWritable e : values) {
      numRecords++;

      if (edgeCutoff > 0 && numRecords > edgeCutoff) {
        continue;
      }

      if (e.get().isSetTitanId()) {
        rightHandTitanId = e.get().getTitanId();
      } else {
        TEdge tEdge = e.get().getTEdge().deepCopy();
        if (rightHandTitanId == null) {
          context.getCounter(Counters.NO_TITAN_ID_FOUND_IN_TIME).increment(1l);
          willPrint = true;
        } else {
          if (!tEdge.isSetLeftTitanId()) {
            throw new RuntimeException("leftTitanId was unset..." + tEdge.toString());
          }

          if (!tEdge.isSetLabel()) {
            throw new RuntimeException("label was unset..." + tEdge.toString());
          }

          if (!tEdge.isSetLeftTitanId()) {
            throw new RuntimeException("label was unset..." + tEdge.toString());
          }
          tEdge.setRightTitanId(rightHandTitanId);
          context.write(new LongWritable(tEdge.getLeftTitanId()), new TEdgeWritable(tEdge));
        }
      }
    }
    if (willPrint && rightHandTitanId != null) {
      context.getCounter(Counters.TITAN_ID_CAME_LATE).increment(1l);
    }

    if (edgeCutoff > 0 && numRecords > edgeCutoff) {
      System.out.println("Dropped " + (numRecords - edgeCutoff) + " edges.");
      context.getCounter(Counters.VERTEX_WITH_DROPPED_EDGES).increment(1l);
      context.getCounter(Counters.DROPPED_EDGES).increment((numRecords - edgeCutoff));
    }

    if (rightHandTitanId == null) {
      if (willPrint) {
        System.out.println("rightHandTitanId == null");
      }
      context.getCounter(Counters.NO_RIGHT_TITAN_ID_IN_REDUCE).increment(1l);
    } else {
      if (willPrint) {
        System.out.println("rightHandTitanId not null");
      }
    }
  }
}