package rhino.etl.hadoop.graph.insert;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;
import junit.framework.TestCase;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;
import org.apache.hadoop.mrunit.types.Pair;
import rhino.etl.hadoop.graph.thrift.Item;
import rhino.etl.hadoop.graph.thrift.TEdge;
import rhino.etl.hadoop.graph.thrift.TEdgeOrTitanId;
import rhino.etl.hadoop.graph.thrift.TVertex;
import rhino.etl.hadoop.graph.writable.RhinoIdWritable;
import rhino.etl.hadoop.graph.writable.TEdgeOrTitanIdWritable;
import rhino.etl.hadoop.graph.writable.TVertexWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
public class VertexInsertMapperTest extends TestCase {
  private Mapper<LongWritable, TVertexWritable, RhinoIdWritable, TEdgeOrTitanIdWritable> mapper;
  private MapDriver<LongWritable, TVertexWritable, RhinoIdWritable, TEdgeOrTitanIdWritable> driver;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() {
    Configuration conf = new Configuration();
    conf.setBoolean("titan.test", true);
    conf.set("titan.storage.directory", folder.getRoot().getAbsolutePath());

    mapper = new VertexInsertMapper();
    driver = new MapDriver<LongWritable, TVertexWritable, RhinoIdWritable, TEdgeOrTitanIdWritable>(mapper)
                   .withConfiguration(conf);
  }

  public TitanGraph getGraph() {
    org.apache.commons.configuration.Configuration conf = new BaseConfiguration();
    conf.setProperty("storage.directory", folder.getRoot().getAbsolutePath());
    return TitanFactory.open(conf);
  }

  public List<Vertex> getVerticies(TitanGraph graph) {
    List<Vertex> verticies = new ArrayList<Vertex>();
    for (Vertex vv : graph.getVertices()) {
      verticies.add(vv);
    }

    return verticies;
  }

  public void testBasicProperties() throws IOException {
    TVertex v = new TVertex();
    v.setRhinoId(123L);
    v.putToProperties("some", Item.string_value("thing"));
    v.putToProperties("someOther", Item.string_value("thing2"));

    driver.withInput(new LongWritable(123), new TVertexWritable(v)).run();

    TitanGraph graph = getGraph();
    List<Vertex> verticies = getVerticies(graph);

    assertEquals(1, verticies.size());

    Vertex actual = verticies.get(0);
    assertEquals("thing", actual.getProperty("some"));
    assertEquals("thing2", actual.getProperty("someOther"));
    assertEquals(123L, actual.getProperty("rhinoId"));

    graph.shutdown();
  }

  public void testBasicEdge() throws IOException {
    TVertex v = new TVertex();
    v.setRhinoId(123L);

    {
      TEdge e = new TEdge();
      e.setRightRhinoId(400L);
      e.putToProperties("some", Item.string_value("thing!"));
      v.addToOutEdges(e);
    }

    List<Pair<RhinoIdWritable, TEdgeOrTitanIdWritable>> results =
        driver.withInput(new LongWritable(123), new TVertexWritable(v)).run();
    TitanGraph graph = getGraph();
    List<Vertex> verticies = getVerticies(graph);

    TEdgeOrTitanId result0 = results.get(0).getSecond().get();
    assertEquals(123L, results.get(0).getFirst().rhinoId);
    assertEquals(false, results.get(0).getFirst().hasEdge);
    assertTrue(result0.isSetTitanId());
    assertEquals(verticies.get(0).getId(), (Long) result0.getTitanId());

    TEdgeOrTitanId result1 = results.get(1).getSecond().get();
    assertEquals(400L, results.get(1).getFirst().rhinoId);
    assertEquals(true, results.get(1).getFirst().hasEdge);
    assertTrue(result1.isSetTEdge());

    assertEquals(result1.getTEdge().getRightRhinoId(), 400L);
    assertEquals(verticies.get(0).getId(), result1.getTEdge().getLeftTitanId());
    assertEquals("thing!", result1.getTEdge().properties.get("some").getString_value());

    graph.shutdown();
  }
}
