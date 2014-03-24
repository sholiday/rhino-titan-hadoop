package rhino.etl.hadoop.thrift;
// Copyright 2013
// Author: Stephen Holiday (stephen.holiday@gmail.com)

import rhino.etl.hadoop.example.thrift.Person;

public class PersonWritable extends ThriftWritable<Person> {
  public PersonWritable() {
    super(new Person());
  }

  public PersonWritable(final Person p) {
    super(new Person(p));
  }
}
