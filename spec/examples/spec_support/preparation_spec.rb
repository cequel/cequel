require_relative "../spec_helper"

require "cequel/spec_support"

describe Cequel::SpecSupport::Preparation do
  subject(:prep) { described_class.instance }
  let(:keyspace) { cequel }

  it "returns itself from #drop_keyspace" do
    expect(prep.drop_keyspace).to eq prep
  end

  it "returns itself from #create_keyspace" do
    expect(prep.create_keyspace).to eq prep
  end

  it "returns itself from #sync_schema" do
    expect(prep.sync_schema).to eq prep
  end


  context "existing keyspace" do
    it "can be deleted" do
      prep.drop_keyspace
      expect(keyspace.exists?).to eq false
    end

    it "doesn't cause failure upon creation request" do
      expect{ prep.create_keyspace }.not_to raise_error
      expect(keyspace.exists?).to eq true
    end

    it "allows tables to be synced" do
      3.times do GC.start end # get rid of most of the crufty classes

      table_name = "model_in_nonstandard_place_" + SecureRandom.hex(4)
      rec_class = Class.new do
        include Cequel::Record
        self.table_name = table_name
        key :sk, :uuid
      end

      prep.sync_schema
      expect(keyspace).to contain_table table_name
    end
  end

  context "keyspace doesn't exist" do
    before do
       Cequel::Record.connection.schema.drop!
    end

    it "doesn't cause failure upon drop requests" do
      expect{ prep.drop_keyspace }.not_to raise_error
    end

    it "allows keyspace can be created" do
      prep.create_keyspace
      expect(keyspace).to exist
    end

    it "causes #sync_schema to fail" do
      expect{ prep.sync_schema }.to raise_error
    end
  end

  # background

  after { Cequel::Record.connection.schema.create! rescue nil }

  matcher :contain_table do |table_name|
    match do |keyspace|
      keyspace.execute(<<-CQL).any?
        SELECT columnfamily_name
        FROM System.schema_columnfamilies
        WHERE keyspace_name='#{keyspace.name}'
          AND columnfamily_name='#{table_name}'
      CQL
    end
  end
end
