package rhino.etl.hadoop.graph.insert;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import rhino.etl.hadoop.graph.thrift.Item;
import rhino.etl.hadoop.graph.thrift.TEdge;
import rhino.etl.hadoop.graph.thrift.TEdgeOrTitanId;
import rhino.etl.hadoop.graph.thrift.TVertex;
import rhino.etl.hadoop.graph.writable.RhinoIdWritable;
import rhino.etl.hadoop.graph.writable.TEdgeOrTitanIdWritable;
import rhino.etl.hadoop.graph.writable.TVertexWritable;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class VertexInsertMapper
    extends Mapper<LongWritable, TVertexWritable, RhinoIdWritable, TEdgeOrTitanIdWritable> {

  public enum Counters {
    VERTICES_WRITTEN,
    VERTEX_PROPERTIES_WRITTEN,
    SUCCESSFUL_TRANSACTIONS,
    FAILED_TRANSACTIONS,
    ROLLED_TRANSACTIONS,
    SHELL_VERTICES_WRITTEN,
    EDGES_PRODUCED
  }

  TitanGraph graph = null;
  Random rng = new Random();
  boolean doNotInsert = false;

  @Override
  public void setup(final Mapper.Context context) throws IOException, InterruptedException {
    org.apache.commons.configuration.Configuration conf = new BaseConfiguration();

    doNotInsert = context.getConfiguration().getBoolean("doNotInsert", false);

    if (!doNotInsert) {
      if (context.getConfiguration().getBoolean("titan.test", false)) {
        conf.setProperty("storage.directory",
                        context.getConfiguration().get("titan.storage.directory"));

      } else {
        conf.setProperty("storage.backend","cassandra");
        if (context.getConfiguration().get("titanHost") != null) {
          conf.setProperty("storage.hostname", context.getConfiguration().get("titanHost"));
        } else {
          throw new IOException("Please specify a titan host.");
        }
        conf.setProperty("storage.batch-loading","true");
        conf.setProperty("storage.machine-id-appendix", rng.nextInt());
        conf.setProperty("ids.block-size", 20000);
        conf.setProperty("autotype","none");
      }
      graph = TitanFactory.open(conf);
    }
  }

  protected void map(LongWritable key, TVertexWritable tVertexWritable,
                     Mapper<LongWritable, TVertexWritable, RhinoIdWritable, TEdgeOrTitanIdWritable>.Context context)
      throws IOException, InterruptedException {
    try {
      final TVertex tVertex = tVertexWritable.get();

      Long leftTitanId = rng.nextLong();
      Vertex v = null;
      if (!doNotInsert) {
        v = graph.addVertex(tVertex.rhinoId);
        context.getCounter(Counters.VERTICES_WRITTEN).increment(1l);
        leftTitanId = (Long) v.getId();
      }

      context.write(new RhinoIdWritable(tVertex.getRhinoId(), false),
          new TEdgeOrTitanIdWritable(TEdgeOrTitanId.titanId(leftTitanId)));
      context.getCounter(Counters.SHELL_VERTICES_WRITTEN).increment(1l);

      // Properties
      if (tVertex.isSetProperties() && !doNotInsert) {
        v.setProperty("rhinoId", tVertex.getRhinoId());
        for (final Map.Entry<String, Item> property : tVertex.getProperties().entrySet()) {
          v.setProperty(property.getKey(), property.getValue().getFieldValue());
          context.getCounter(Counters.VERTEX_PROPERTIES_WRITTEN).increment(1l);
        }
      }

      // Edges
      if (tVertex.isSetOutEdges()) {
        for (final TEdge tEdge : tVertex.getOutEdges()) {
          TEdge edge = new TEdge(tEdge);
          edge.setLeftTitanId(leftTitanId);

          context.getCounter(Counters.EDGES_PRODUCED).increment(1L);
          context.write(new RhinoIdWritable(tEdge.getRightRhinoId(), true),
              new TEdgeOrTitanIdWritable(TEdgeOrTitanId.tEdge(edge)));
        }

      }

    } catch (final Exception e) {
      if (!doNotInsert) {
        graph.rollback();
        context.getCounter(Counters.ROLLED_TRANSACTIONS).increment(1l);
        context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
      }
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public void cleanup(final Mapper<LongWritable, TVertexWritable, RhinoIdWritable, TEdgeOrTitanIdWritable>.Context
                            context) throws IOException, InterruptedException {
    if (!doNotInsert && this.graph instanceof TransactionalGraph) {
      try {
        ((TransactionalGraph) this.graph).commit();
        context.getCounter(Counters.SUCCESSFUL_TRANSACTIONS).increment(1l);
      } catch (Exception e) {
        ((TransactionalGraph) this.graph).rollback();
        context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
        throw new IOException(e.getMessage(), e);
      }
    }
    if (!doNotInsert) {
      this.graph.shutdown();
    }
  }
}
