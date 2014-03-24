package rhino.etl.hadoop.graph.insert;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import rhino.etl.hadoop.graph.thrift.Item;
import rhino.etl.hadoop.graph.thrift.TEdge;
import rhino.etl.hadoop.graph.writable.TEdgeWritable;

import java.io.IOException;
import java.util.Random;

public class EdgeInsertMapper extends Mapper<LongWritable, TEdgeWritable,
    LongWritable,
    NullWritable> {

  public enum Counters {
    VERTICES_RETRIEVED,
    EDGES_WRITTEN,
    EDGE_PROPERTIES_WRITTEN,
    SUCCESSFUL_TRANSACTIONS,
    FAILED_TRANSACTIONS,
    ROLLED_TRANSACTIONS,
    LEFT_VERTICES_MISSING,
    RIGHT_VERTICES_MISSING
  }

  TitanGraph graph = null;
  long edgesInTransaction = 0;

  @Override
  public void setup(final Context context) throws IOException, InterruptedException {
    org.apache.commons.configuration.Configuration conf = new BaseConfiguration();
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
      conf.setProperty("storage.machine-id-appendix", (new Random()).nextInt());
      conf.setProperty("ids.block-size", 20000);
      conf.setProperty("autotype","none");

    }
    System.out.println(conf.getString("storage.hostname"));
    graph = TitanFactory.open(conf);
    edgesInTransaction = 0;
  }

  @Override
  protected void map(LongWritable key, TEdgeWritable tEdgeWritable,
                     Mapper<LongWritable, TEdgeWritable, LongWritable, NullWritable>.Context
                         context)
      throws IOException, InterruptedException {
    edgesInTransaction++;
    try {
      final TEdge tEdge = tEdgeWritable.get();

      if (!tEdge.isSetLeftTitanId()) {
        throw new RuntimeException("leftTitanId was unset..." + tEdge.toString());
      }

      if (!tEdge.isSetLabel()) {
        throw new RuntimeException("label was unset..." + tEdge.toString());
      }

      if (!tEdge.isSetRightTitanId()) {
        throw new RuntimeException("rightTitanId was unset..." + tEdge.toString());
      }

      Vertex leftVertex = graph.getVertex(tEdge.leftTitanId);
      Vertex rightVertex = graph.getVertex(tEdge.rightTitanId);

      if (leftVertex == null) {
        context.getCounter(Counters.LEFT_VERTICES_MISSING).increment(1L);
        return;
      }
      if (rightVertex == null) {
        context.getCounter(Counters.RIGHT_VERTICES_MISSING).increment(1L);
        return;
      }
      context.getCounter(Counters.VERTICES_RETRIEVED).increment(2L);

      Edge bEdge = graph.addEdge(tEdge, leftVertex, rightVertex, tEdge.getLabel());
      if (tEdge.isSetProperties()) {
        for (final java.util.Map.Entry<String, Item> entry : tEdge.properties.entrySet()) {
          bEdge.setProperty(entry.getKey(), entry.getValue().getFieldValue());
          context.getCounter(Counters.EDGE_PROPERTIES_WRITTEN).increment(1L);
        }
      }
      context.getCounter(Counters.EDGES_WRITTEN).increment(1L);

    } catch (final Exception e) {
      System.out.println("edgesInTransaction = " + edgesInTransaction);
      System.out.println("Had to rollback transaction.");
      graph.rollback();
      context.getCounter(Counters.ROLLED_TRANSACTIONS).increment(1l);
      context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public void cleanup(final Context context) throws IOException, InterruptedException {
    System.out.println("edgesInTransaction = " + edgesInTransaction);
    edgesInTransaction = 0;
    if (this.graph instanceof TransactionalGraph) {
      try {
        ((TransactionalGraph) this.graph).commit();
        context.getCounter(Counters.SUCCESSFUL_TRANSACTIONS).increment(1L);
      } catch (Exception e) {
        System.out.println("Could not commit transaction during Map.cleanup()");
        // LOGGER.error("Could not commit transaction during Map.cleanup():", e);
        ((TransactionalGraph) this.graph).rollback();
        context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1L);
        throw new IOException(e.getMessage(), e);
      }
    }
    this.graph.shutdown();
  }
}