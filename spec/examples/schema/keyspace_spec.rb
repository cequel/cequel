# -*- encoding : utf-8 -*-
require File.expand_path('../../spec_helper', __FILE__)

describe Cequel::Schema::Keyspace do
  let(:keyspace) { cequel.schema }

  describe 'creating keyspace' do
    before do
      cequel.schema.drop! if cequel.schema.exists?
    end

    after do
      cequel.clear_active_connections!
    end

    let(:basic_config) {
      {
        host: Cequel::SpecSupport::Helpers.host,
        port: Cequel::SpecSupport::Helpers.port,
        keyspace: 'totallymadeup'
      }
    }

    let(:schema_config) {
      cequel.client.use('system')
      cequel.client.execute("SELECT * FROM schema_keyspaces WHERE keyspace_name = 'totallymadeup'").first
    }

    context 'with default options' do
      let(:config) { basic_config }

      it 'uses default keyspace configuration' do
        cequel.configure(config)
        keyspace.create!
        expect(schema_config).to eq({
          "keyspace_name"=>"totallymadeup",
          "durable_writes"=>true,
          "strategy_class"=>"org.apache.cassandra.locator.SimpleStrategy",
          "strategy_options"=>"{\"replication_factor\":\"1\"}"
        })
      end
    end

    context 'with explicit options' do
      let(:config) { basic_config }

      it 'uses specified options' do
        cequel.configure(config)
        keyspace.create! replication: { class: "SimpleStrategy", replication_factor: 2 }
        expect(schema_config).to eq({
          "keyspace_name"=>"totallymadeup",
          "durable_writes"=>true,
          "strategy_class"=>"org.apache.cassandra.locator.SimpleStrategy",
          "strategy_options"=>"{\"replication_factor\":\"2\"}"
        })
      end
    end

    context 'keeping compatibility' do
      let(:config) { basic_config }

      it 'accepts class and replication_factor options' do
        cequel.configure(config)
        keyspace.create! class: "SimpleStrategy", replication_factor: 2
        expect(schema_config).to eq({
          "keyspace_name"=>"totallymadeup",
          "durable_writes"=>true,
          "strategy_class"=>"org.apache.cassandra.locator.SimpleStrategy",
          "strategy_options"=>"{\"replication_factor\":\"2\"}"
        })
      end

      it "raises an error if a class other than SimpleStrategy is given" do
        cequel.configure(config)
        expect {
          keyspace.create! class: "NetworkTopologyStrategy", replication_factor: 2
        }.to raise_error('For strategy other than SimpleStrategy, please use the replication option.')
      end
    end

    context 'with custom replication options' do
      let(:config) {
        basic_config.merge(replication: { class: "SimpleStrategy", replication_factor: 3 })
      }

      it 'uses default keyspace configuration' do
        cequel.configure(config)
        keyspace.create!
        expect(schema_config).to eq({
          "keyspace_name"=>"totallymadeup",
          "durable_writes"=>true,
          "strategy_class"=>"org.apache.cassandra.locator.SimpleStrategy",
          "strategy_options"=>"{\"replication_factor\":\"3\"}"
        })
      end
    end

    context 'with another custom replication options' do
      let(:config) {
        basic_config.merge(replication: { class: "NetworkTopologyStrategy", datacenter1: 3, datacenter2: 2 })
      }

      it 'uses default keyspace configuration' do
        cequel.configure(config)
        keyspace.create!
        expect(schema_config).to eq({
          "keyspace_name"=>"totallymadeup",
          "durable_writes"=>true,
          "strategy_class"=>"org.apache.cassandra.locator.NetworkTopologyStrategy",
          "strategy_options"=>"{\"datacenter1\":\"3\",\"datacenter2\":\"2\"}"
        })
      end
    end

    context 'with custom durable_write option' do
      let(:config) {
        basic_config.merge(durable_writes: false)
      }

      it 'uses default keyspace configuration' do
        cequel.configure(config)
        keyspace.create!
        expect(schema_config).to eq({
          "keyspace_name"=>"totallymadeup",
          "durable_writes"=>false,
          "strategy_class"=>"org.apache.cassandra.locator.SimpleStrategy",
          "strategy_options"=>"{\"replication_factor\":\"1\"}"
        })
      end
    end
  end # describe 'creating keyspace'
end
