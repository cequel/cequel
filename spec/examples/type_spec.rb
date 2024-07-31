# -*- encoding : utf-8 -*-
require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Type do

  describe 'ascii' do
    subject { Cequel::Type[:ascii] }
    its(:cql_name) { should == :ascii }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.AsciiType' }

    describe '#cast' do
      specify { expect(subject.cast('hey'.encode('UTF-8')).encoding.name).
        to eq('US-ASCII') }
    end
  end

  describe 'blob' do
    subject { Cequel::Type[:blob] }
    its(:cql_name) { should == :blob }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.BytesType' }

    describe '#cast' do
      specify { expect(subject.cast(123)).to eq(123.to_s(16)) }
      specify { expect(subject.cast(123).encoding.name).to eq('ASCII-8BIT') }
      specify { expect(subject.cast('2345').encoding.name).to eq('ASCII-8BIT') }
    end
  end

  describe 'boolean' do
    subject { Cequel::Type[:boolean] }
    its(:cql_name) { should == :boolean }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.BooleanType' }

    describe '#cast' do
      specify { expect(subject.cast(true)).to eq(true) }
      specify { expect(subject.cast(false)).to eq(false) }
      specify { expect(subject.cast(1)).to eq(true) }
    end
  end

  describe 'counter' do
    subject { Cequel::Type[:counter] }
    its(:cql_name) { should == :counter }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.CounterColumnType' }

    describe '#cast' do
      specify { expect(subject.cast(1)).to eq(1) }
      specify { expect(subject.cast('1')).to eq(1) }
    end
  end

  describe 'decimal' do
    subject { Cequel::Type[:decimal] }
    its(:cql_name) { should == :decimal }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.DecimalType' }

    describe '#cast' do
      specify { expect(subject.cast(1)).to eql(BigDecimal('1.0')) }
      specify { expect(subject.cast(1.0)).to eql(BigDecimal('1.0')) }
      specify { expect(subject.cast(1.0.to_r)).to eql(BigDecimal('1.0')) }
      specify { expect(subject.cast('1')).to eql(BigDecimal('1.0')) }
    end
  end

  describe 'double' do
    subject { Cequel::Type[:double] }
    its(:cql_name) { should == :double }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.DoubleType' }

    describe '#cast' do
      specify { expect(subject.cast(1.0)).to eql(1.0) }
      specify { expect(subject.cast(1)).to eql(1.0) }
      specify { expect(subject.cast(1.0.to_r)).to eql(1.0) }
      specify { expect(subject.cast('1.0')).to eql(1.0) }
      specify { expect(subject.cast(BigDecimal('1.0'))).to eql(1.0) }
    end
  end

  describe 'float' do
    subject { Cequel::Type[:float] }
    its(:cql_name) { should == :float }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.FloatType' }

    describe '#cast' do
      specify { expect(subject.cast(1.0)).to eql(1.0) }
      specify { expect(subject.cast(1)).to eql(1.0) }
      specify { expect(subject.cast(1.0.to_r)).to eql(1.0) }
      specify { expect(subject.cast('1.0')).to eql(1.0) }
      specify { expect(subject.cast(BigDecimal('1.0'))).to eql(1.0) }
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
      specify { expect(subject.cast(1)).to eql(1) }
      specify { expect(subject.cast('1')).to eql(1) }
      specify { expect(subject.cast(1.0)).to eql(1) }
      specify { expect(subject.cast(1.0.to_r)).to eql(1) }
      specify { expect(subject.cast(BigDecimal('1.0'))).to eql(1) }
    end
  end

  describe 'bigint' do
    subject { Cequel::Type[:bigint] }
    its(:cql_name) { should == :bigint }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.LongType' }

    describe '#cast' do
      specify { expect(subject.cast(1)).to eql(1) }
      specify { expect(subject.cast('1')).to eql(1) }
      specify { expect(subject.cast(1.0)).to eql(1) }
      specify { expect(subject.cast(1.0.to_r)).to eql(1) }
      specify { expect(subject.cast(BigDecimal('1.0'))).to eql(1) }
    end
  end

  describe 'text' do
    subject { Cequel::Type[:text] }
    its(:cql_name) { should == :text }
    its(:internal_name) { should == 'org.apache.cassandra.db.marshal.UTF8Type' }
    it { is_expected.to eq(Cequel::Type[:varchar]) }

    describe '#cast' do
      specify { expect(subject.cast('cql')).to eq('cql') }
      specify { expect(subject.cast(1)).to eq('1') }
      specify { expect(subject.cast('cql').encoding.name).to eq('UTF-8') }
      specify { expect(subject.cast('cql'.force_encoding('US-ASCII')).
        encoding.name).to eq('UTF-8') }
    end
  end

  describe 'timestamp' do
    subject { Cequel::Type[:timestamp] }
    its(:cql_name) { should == :timestamp }
    its(:internal_name) { should == 'org.apache.cassandra.db.marshal.DateType' }

    describe '#cast' do
      let(:now) { Time.at(Time.now.to_i) }
      specify { expect(subject.cast(now)).to eq(now) }
      specify { expect(subject.cast(now.to_i)).to eq(now) }
      specify { expect(subject.cast(now.to_s)).to eq(now) }
      specify { expect(subject.cast(now.to_datetime)).to eq(now) }
      specify { expect(subject.cast(now.to_date)).to eq(now.to_date.to_time) }
    end
  end

  describe 'date' do
    subject { Cequel::Type[:date] }
    its(:cql_name) { should == :date }
    its(:internal_name) { should == 'org.apache.cassandra.db.marshal.DateType' }

    describe '#cast' do
      let(:today) { Date.today }
      specify { expect(subject.cast(today)).to eq(today) }
      specify { expect(subject.cast(today.to_s)).to eq(today) }
      specify { expect(subject.cast(today.to_datetime)).to eq(today) }
      specify { expect(subject.cast(today.to_time)).to eq(today) }
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
      let(:uuid) { Cequel.uuid }
      specify { expect(subject.cast(uuid)).to eq(uuid) }
      specify { expect(subject.cast(uuid.to_s)).to eq(uuid) }
      specify { expect(subject.cast(uuid.value)).to eq(uuid) }
      if defined? SimpleUUID::UUID
        specify { expect(subject.cast(SimpleUUID::UUID.new(uuid.value)))
                    .to eq(uuid) }
      end
    end
  end

  describe 'varint' do
    subject { Cequel::Type[:varint] }
    its(:cql_name) { should == :varint }
    its(:internal_name) {
      should == 'org.apache.cassandra.db.marshal.IntegerType' }

    describe '#cast' do
      specify { expect(subject.cast(1)).to eql(1) }
      specify { expect(subject.cast('1')).to eql(1) }
      specify { expect(subject.cast(1.0)).to eql(1) }
      specify { expect(subject.cast(1.0.to_r)).to eql(1) }
      specify { expect(subject.cast(BigDecimal('1.0'))).to eql(1) }
    end
  end

  describe '::quote' do
    [
      ["don't", "'don''t'"],
      ["don't".force_encoding('US-ASCII'), "'don''t'"],
      ["don't".force_encoding('ASCII-8BIT'), "'don''t'"],
      ["3dc49a6".force_encoding('ASCII-8BIT'), "0x3dc49a6"],
      [["one", "two"], "'one','two'"],
      [1, '1'],
      [1.2, '1.2'],
      [true, 'true'],
      [false, 'false'],
      [Time.at(1401323181, 381000), '1401323181381'],
      [Time.at(1401323181, 381999), '1401323181382'],
      [Time.at(1401323181, 381000).in_time_zone, '1401323181381'],
      [Date.parse('2014-05-28'), "1401235200000"],
      [Time.at(1401323181, 381000).to_datetime, '1401323181381'],
      [Cequel.uuid("dbf51e0e-e6c7-11e3-be60-237d76548395"),
       "dbf51e0e-e6c7-11e3-be60-237d76548395"]
    ].each do |input, output|
      specify { expect(Cequel::Type.quote(input)).to eq(output) }
    end
  end

end
