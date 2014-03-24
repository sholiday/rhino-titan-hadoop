package rhino.etl.hadoop.thrift;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import junit.framework.TestCase;
import rhino.etl.hadoop.example.thrift.Person;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.*;

@RunWith(PowerMockRunner.class)
public class ThriftWritableTest extends TestCase {

  Person p;

  public void setUp() {
    p = new Person();
    p.setId(42);
    p.setName("bob is cool");
  }


  public void testRoundTrip() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutput doo = new DataOutputStream(os);

    ThriftWritable<Person> writableOut = new PersonWritable(p);
    writableOut.write(doo);

    InputStream is = new ByteArrayInputStream(os.toByteArray());
    DataInput di = new DataInputStream(is);

    PersonWritable writableIn = new PersonWritable();
    writableIn.readFields(di);

    assertEquals(p.getId(), writableIn.get().getId());
    assertEquals(p.getName(), writableIn.get().getName());
  }
}
