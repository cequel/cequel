require_relative "../spec_helper"

require "cequel/spec_support"

describe Cequel::SpecSupport::Preparation do
  subject(:prep) { described_class.new([], quiet: true) }
  let(:keyspace) { cequel }

  it "returns itself from #drop_keyspace" do
    expect(prep.drop_keyspace).to eq prep
  end

  it "returns itself from #create_keyspace", :retry => 1, :retry_wait => 1 do
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
    before(:each) do
      Cequel::Record.connection.schema.drop!
    end

    let!(:model) {
      Class.new do
        include Cequel::Record
        self.table_name = "blog_" + SecureRandom.hex(4)
        key :name, :text
      end
    }

    it "doesn't cause failure upon drop requests" do
      expect{ prep.drop_keyspace }.not_to raise_error
    end

    it "allows keyspace can be created" do
      prep.create_keyspace
      keyspace.cluster.refresh_schema
      expect(keyspace).to exist
    end

    it "causes #sync_schema to fail" do
      expect{ prep.sync_schema }.to raise_error(Cequel::NoSuchKeyspaceError)
    end
  end

  # background

  before(:each) do
    Cequel::Record.forget_all_descendants!
  end

  after(:each) do
    begin
      Cequel::Record.connection.clear_active_connections!
      Cequel::Record.connection.schema.create!
    rescue
      nil
    end
  end

  matcher :contain_table do |table_name|
    match do |keyspace|
      keyspace.cluster.refresh_schema
      ks = keyspace.cluster.keyspace(keyspace.name)

      ks && ks.has_table?(table_name)
    end

    failure_message do |keyspace|
      if keyspace.cluster.has_keyspace?(keyspace.name)
        "no such keyspace #{keyspace.name}"
      else
        "no such table #{keyspace.name}.#{table_name}"
      end
    end
  end
end
