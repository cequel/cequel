require_relative "../spec_helper"

require "cequel/spec_support"

describe Cequel::SpecSupport::Preparation do
  subject(:prep) { described_class.instance }
  let(:keyspace) { cequel }

  context "existing keyspace" do
    it "can be deleted" do
      prep.drop_keyspace
      expect(keyspace).not_to exist
    end

    it "doesn't cause failure upon creation request" do
      expect{ prep.create_keyspace }.not_to raise_error
      expect(keyspace).to exist
    end

    it "allows tables to be synced" do
      rec_class = Class.new do
        include Cequel::Record
        self.table_name = "model_in_nonstandard_place"
        key :sk, :uuid
      end

      prep.sync_schema
      expect(keyspace).to contain_table "model_in_nonstandard_place"
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

  matcher :exist do
    match do |keyspace|
      keyspace.clear_active_connections!

      begin
        keyspace.execute "SELECT now() FROM system.local"
        true
      rescue Cql::QueryError => e
        if /exist/i === e.message
          false
        else
          raise # something really went wrong
        end
      end
    end
  end
end
