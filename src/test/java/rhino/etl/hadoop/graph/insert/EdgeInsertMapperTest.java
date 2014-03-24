package rhino.etl.hadoop.graph.insert;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import junit.framework.TestCase;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;
import rhino.etl.hadoop.graph.thrift.TEdge;
import rhino.etl.hadoop.graph.writable.TEdgeWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
public class EdgeInsertMapperTest extends TestCase {
  private Mapper<LongWritable, TEdgeWritable, LongWritable, NullWritable> mapper;
  private MapDriver<LongWritable, TEdgeWritable, LongWritable, NullWritable> driver;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() {
    Configuration conf = new Configuration();
    conf.setBoolean("titan.test", true);
    conf.set("titan.storage.directory", folder.getRoot().getAbsolutePath());

    mapper = new EdgeInsertMapper();
    driver = new MapDriver<LongWritable, TEdgeWritable, LongWritable, NullWritable>(mapper)
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

  // Test creating an edge from 100 to 101. (100 -> 101)
  public void testBasic() throws IOException {
    TitanGraph g = getGraph();

    Vertex v0 = g.addVertex(100L);
    v0.setProperty("rhinoId", 100L);
    long v0TitanId = (Long) v0.getId();

    Vertex v1 = g.addVertex(101L);
    v1.setProperty("rhinoId", 101L);
    long v1TitanId = (Long) v1.getId();

    g.shutdown();

    TEdge e = new TEdge();
    e.setLeftRhinoId(100L);
    e.setLeftTitanId(v0TitanId);
    e.setRightTitanId(v1TitanId);
    e.setLabel("someEdge");

    driver.withInput(new LongWritable(123), new TEdgeWritable(e)).run();

    g = getGraph();
    Vertex right = g.getVertex(v0TitanId).getVertices(Direction.OUT, "someEdge").iterator().next();
    assertEquals(101L, right.getProperty("rhinoId"));
    assertEquals(v1TitanId, right.getId());
    g.shutdown();
  }

}
