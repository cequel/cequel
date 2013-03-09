require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Type do

  describe 'ascii' do
    subject { Cequel::Type[:ascii] }
    specify { subject.cql_name.should == :ascii }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.AsciiType' }
  end

  describe 'blob' do
    subject { Cequel::Type[:blob] }
    specify { subject.cql_name.should == :blob }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.BytesType' }
  end

  describe 'boolean' do
    subject { Cequel::Type[:boolean] }
    specify { subject.cql_name.should == :boolean }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.BooleanType' }
  end

  describe 'counter' do
    subject { Cequel::Type[:counter] }
    specify { subject.cql_name.should == :counter }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.CounterColumnType' }
  end

  describe 'decimal' do
    subject { Cequel::Type[:decimal] }
    specify { subject.cql_name.should == :decimal }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.DecimalType' }
  end

  describe 'double' do
    subject { Cequel::Type[:double] }
    specify { subject.cql_name.should == :double }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.DoubleType' }
  end

  describe 'float' do
    subject { Cequel::Type[:float] }
    specify { subject.cql_name.should == :float }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.FloatType' }
  end

  describe 'inet' do
    subject { Cequel::Type[:inet] }
    specify { subject.cql_name.should == :inet }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.InetAddressType' }
  end

  describe 'int' do
    subject { Cequel::Type[:int] }
    specify { subject.cql_name.should == :int }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.Int32Type' }
  end

  describe 'long' do
    subject { Cequel::Type[:long] }
    specify { subject.cql_name.should == :long }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.LongType' }
  end

  describe 'text' do
    subject { Cequel::Type[:text] }
    specify { subject.cql_name.should == :text }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.UTF8Type' }
    specify { subject.should == Cequel::Type[:varchar] }
  end

  describe 'timestamp' do
    subject { Cequel::Type[:timestamp] }
    specify { subject.cql_name.should == :timestamp }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.DateType' }
  end

  describe 'timeuuid' do
    subject { Cequel::Type[:timeuuid] }
    specify { subject.cql_name.should == :timeuuid }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.TimeUUIDType' }
  end

  describe 'uuid' do
    subject { Cequel::Type[:uuid] }
    specify { subject.cql_name.should == :uuid }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.UUIDType' }
  end

  describe 'varint' do
    subject { Cequel::Type[:varint] }
    specify { subject.cql_name.should == :varint }
    specify { subject.internal_name.should ==
      'org.apache.cassandra.db.marshal.IntegerType' }
  end

end
