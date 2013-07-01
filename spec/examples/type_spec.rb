require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Type do

  describe 'ascii' do
    subject { Cequel::Type[:ascii] }
    its(:cql_name) { should == :ascii }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.AsciiType' }

    describe '#cast' do
      specify { subject.cast('hey'.encode('UTF-8')).encoding.name.
        should == 'US-ASCII' }
    end
  end

  describe 'blob' do
    subject { Cequel::Type[:blob] }
    its(:cql_name) { should == :blob }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.BytesType' }

    describe '#cast' do
      specify { subject.cast(123).should == 123.to_s(16) }
      specify { subject.cast(123).encoding.name.should == 'ASCII-8BIT' }
      specify { subject.cast('2345').encoding.name.should == 'ASCII-8BIT' }
    end
  end

  describe 'boolean' do
    subject { Cequel::Type[:boolean] }
    its(:cql_name) { should == :boolean }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.BooleanType' }

    describe '#cast' do
      specify { subject.cast(true).should == true }
      specify { subject.cast(false).should == false }
      specify { subject.cast(1).should == true }
    end
  end

  describe 'counter' do
    subject { Cequel::Type[:counter] }
    its(:cql_name) { should == :counter }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.CounterColumnType' }

    describe '#cast' do
      specify { subject.cast(1).should == 1 }
      specify { subject.cast('1').should == 1 }
    end
  end

  describe 'decimal' do
    subject { Cequel::Type[:decimal] }
    its(:cql_name) { should == :decimal }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.DecimalType' }

    describe '#cast' do
      specify { subject.cast(1).should eql(BigDecimal.new('1.0')) }
      specify { subject.cast(1.0).should eql(BigDecimal.new('1.0')) }
      specify { subject.cast(1.0.to_r).should eql(BigDecimal.new('1.0')) }
      specify { subject.cast('1').should eql(BigDecimal.new('1.0')) }
    end
  end

  describe 'double' do
    subject { Cequel::Type[:double] }
    its(:cql_name) { should == :double }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.DoubleType' }

    describe '#cast' do
      specify { subject.cast(1.0).should eql(1.0) }
      specify { subject.cast(1).should eql(1.0) }
      specify { subject.cast(1.0.to_r).should eql(1.0) }
      specify { subject.cast('1.0').should eql(1.0) }
      specify { subject.cast(BigDecimal.new('1.0')).should eql(1.0) }
    end
  end

  describe 'float' do
    subject { Cequel::Type[:float] }
    its(:cql_name) { should == :float }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.FloatType' }

    describe '#cast' do
      specify { subject.cast(1.0).should eql(1.0) }
      specify { subject.cast(1).should eql(1.0) }
      specify { subject.cast(1.0.to_r).should eql(1.0) }
      specify { subject.cast('1.0').should eql(1.0) }
      specify { subject.cast(BigDecimal.new('1.0')).should eql(1.0) }
    end
  end

  describe 'inet' do
    subject { Cequel::Type[:inet] }
    its(:cql_name) { should == :inet }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.InetAddressType' }
  end

  describe 'int' do
    subject { Cequel::Type[:int] }
    its(:cql_name) { should == :int }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.Int32Type' }

    describe '#cast' do
      specify { subject.cast(1).should eql(1) }
      specify { subject.cast('1').should eql(1) }
      specify { subject.cast(1.0).should eql(1) }
      specify { subject.cast(1.0.to_r).should eql(1) }
      specify { subject.cast(BigDecimal.new('1.0')).should eql(1) }
    end
  end

  describe 'long' do
    subject { Cequel::Type[:long] }
    its(:cql_name) { should == :long }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.LongType' }

    describe '#cast' do
      specify { subject.cast(1).should eql(1) }
      specify { subject.cast('1').should eql(1) }
      specify { subject.cast(1.0).should eql(1) }
      specify { subject.cast(1.0.to_r).should eql(1) }
      specify { subject.cast(BigDecimal.new('1.0')).should eql(1) }
    end
  end

  describe 'text' do
    subject { Cequel::Type[:text] }
    its(:cql_name) { should == :text }
    its(:internal_name) { should == 'org.apache.cassandra.db.marshal.UTF8Type' }
    it { should == Cequel::Type[:varchar] }

    describe '#cast' do
      specify { subject.cast('cql').should == 'cql' }
      specify { subject.cast(1).should == '1' }
      specify { subject.cast('cql').encoding.name.should == 'UTF-8' }
      specify { subject.cast('cql'.force_encoding('US-ASCII')).
        encoding.name.should == 'UTF-8' }
    end
  end

  describe 'timestamp' do
    subject { Cequel::Type[:timestamp] }
    its(:cql_name) { should == :timestamp }
    its(:internal_name) { should == 'org.apache.cassandra.db.marshal.DateType' }

    describe '#cast' do
      let(:now) { Time.at(Time.now.to_i) }
      specify { subject.cast(now).should == now }
      specify { subject.cast(now.to_i).should == now }
      specify { subject.cast(now.to_s).should == now }
      specify { subject.cast(now.to_datetime).should == now }
      specify { subject.cast(now.to_date).should == now.to_date.to_time }
    end
  end

  describe 'timeuuid' do
    subject { Cequel::Type[:timeuuid] }
    its(:cql_name) { should == :timeuuid }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.TimeUUIDType' }
  end

  describe 'uuid' do
    subject { Cequel::Type[:uuid] }
    its(:cql_name) { should == :uuid }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.UUIDType' }

    describe '#cast' do
      let(:uuid) { CassandraCQL::UUID.new }
      specify { subject.cast(uuid).should == uuid }
      specify { subject.cast(SimpleUUID::UUID.new(uuid)).
        should be_a(CassandraCQL::UUID) }
      specify { subject.cast(uuid.to_guid).should == uuid }
      specify { subject.cast(uuid.to_i).should == uuid }
    end
  end

  describe 'varint' do
    subject { Cequel::Type[:varint] }
    its(:cql_name) { should == :varint }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.IntegerType' }

    describe '#cast' do
      specify { subject.cast(1).should eql(1) }
      specify { subject.cast('1').should eql(1) }
      specify { subject.cast(1.0).should eql(1) }
      specify { subject.cast(1.0.to_r).should eql(1) }
      specify { subject.cast(BigDecimal.new('1.0')).should eql(1) }
    end
  end

end
