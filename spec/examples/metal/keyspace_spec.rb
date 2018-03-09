# -*- encoding : utf-8 -*-
require_relative '../spec_helper'

describe Cequel::Metal::Keyspace do
  before :all do
    cequel.schema.create_table(:posts) do
      key :id, :int
      column :title, :text
      column :body, :text
    end
  end

  after :each do
    ids = cequel[:posts].select(:id).map { |row| row[:id] }
    cequel[:posts].where(id: ids).delete if ids.any?
  end

  after :all do
    cequel.schema.drop_table(:posts)
  end

  describe '::batch' do
    it 'should send enclosed write statements in bulk' do
      expect_statement_count 1 do
        cequel.batch do
          cequel[:posts].insert(id: 1, title: 'Hey')
          cequel[:posts].where(id: 1).update(body: 'Body')
          cequel[:posts].where(id: 1).delete(:title)
        end
      end
      expect(cequel[:posts].first).to eq({id: 1, title: nil, body: 'Body'}
        .with_indifferent_access)
    end

    it 'should auto-apply if option given' do
      cequel.batch(auto_apply: 2) do
        cequel[:posts].insert(id: 1, title: 'One')
        expect(cequel[:posts].to_a.count).to be_zero
        cequel[:posts].insert(id: 2, title: 'Two')
        expect(cequel[:posts].to_a.count).to be(2)
      end
    end

    it 'should do nothing if no statements executed in batch' do
      expect { cequel.batch {} }.to_not raise_error
    end

    it 'should execute unlogged batch if specified' do
      expect_query_with_consistency(instance_of(Cassandra::Statements::Batch::Unlogged), anything) do
        cequel.batch(unlogged: true) do
          cequel[:posts].insert(id: 1, title: 'One')
          cequel[:posts].insert(id: 2, title: 'Two')
        end
      end
    end

    it 'should execute batch with given consistency' do
      expect_query_with_consistency(instance_of(Cassandra::Statements::Batch::Logged), :one) do
        cequel.batch(consistency: :one) do
          cequel[:posts].insert(id: 1, title: 'One')
          cequel[:posts].insert(id: 2, title: 'Two')
        end
      end
    end

    it 'should raise error if consistency specified in individual query in batch' do
      expect {
        cequel.batch(consistency: :one) do
          cequel[:posts].consistency(:quorum).insert(id: 1, title: 'One')
        end
      }.to raise_error(ArgumentError)
    end
  end

  describe "#exists?" do
    it "is true for existent keyspaces", :retry => 1, :retry_wait => 1 do
      expect(cequel.exists?).to eq true
    end

    it "is false for non-existent keyspaces" do
      nonexistent_keyspace = Cequel.connect host: Cequel::SpecSupport::Helpers.host,
                           port: Cequel::SpecSupport::Helpers.port,
                           keyspace: "totallymadeup"

      expect(nonexistent_keyspace.exists?).to be false
    end
  end

  describe "#drop_table", cql: "~> 3.1" do
    it "allows IF EXISTS" do
      expect { cequel.schema.drop_table(:unknown) }.to raise_error(Cassandra::Errors::InvalidError)
      expect { cequel.schema.drop_table(:unknown, exists: true) }.not_to raise_error
    end
  end

  describe "#drop_materialized_view", cql: "~> 3.4" do
    it "allows IF EXISTS" do
      expect { cequel.schema.drop_materialized_view(:unknown) }.to raise_error(Cassandra::Errors::ConfigurationError)
      expect { cequel.schema.drop_materialized_view(:unknown, exists: true) }.not_to raise_error
    end
  end

  describe "#ssl_config" do
    it "ssl configuration settings get extracted correctly for sending to cluster" do
      connect = Cequel.connect host: Cequel::SpecSupport::Helpers.host,
                           port: Cequel::SpecSupport::Helpers.port,
                           ssl: true,
                           server_cert: 'path/to/server_cert',
                           client_cert: 'path/to/client_cert',
                           private_key: 'private_key',
                           passphrase: 'passphrase'

      expect(connect.ssl_config[:ssl]).to be true
      expect(connect.ssl_config[:server_cert]).to eq('path/to/server_cert')
      expect(connect.ssl_config[:client_cert]).to eq('path/to/client_cert')
      expect(connect.ssl_config[:private_key]).to eq('private_key')
      expect(connect.ssl_config[:passphrase]).to eq('passphrase')
    end
  end

  describe "#client_compression" do
    let(:client_compression) { :lz4 }
    let(:connect) do
      Cequel.connect host: Cequel::SpecSupport::Helpers.host,
          port: Cequel::SpecSupport::Helpers.port,
          client_compression: client_compression
    end
    it "client compression settings get extracted correctly for sending to cluster" do
      expect(connect.client_compression).to eq client_compression
    end
  end

  describe '#cassandra_options' do
    let(:cassandra_options) { {foo: :bar} }
    let(:connect) do
      Cequel.connect host: Cequel::SpecSupport::Helpers.host,
          port: Cequel::SpecSupport::Helpers.port,
          cassandra_options: cassandra_options
    end
    it 'passes the cassandra options as part of the client options' do
      expect(connect.send(:client_options)).to have_key(:foo)
    end
  end

  describe 'cassandra error handling' do
    let(:connect_options) do
      {
        host: Cequel::SpecSupport::Helpers.host,
          port: Cequel::SpecSupport::Helpers.port
      }
    end

    let(:default_connect) do
      Cequel.connect(connect_options)
    end

    class SpecCassandraErrorHandler
      def initialize(options = {})
      end

      def execute_stmt(keyspace)
        yield
      end
    end

    it 'uses the error handler passed in as a string' do
      obj = Cequel.connect connect_options.merge(
          cassandra_error_policy: 'SpecCassandraErrorHandler')

      expect(obj.error_policy.class).to equal(SpecCassandraErrorHandler)
    end

    it 'uses the error handler passed in as a module' do
      obj = Cequel.connect connect_options.merge(
          cassandra_error_policy: SpecCassandraErrorHandler)

      expect(obj.error_policy.class).to equal(SpecCassandraErrorHandler)
    end

    it 'uses the instance of an error handler passed in' do
      policy = SpecCassandraErrorHandler.new

      obj = Cequel.connect connect_options.merge(
          cassandra_error_policy: policy)

      expect(obj.error_policy).to equal(policy)
    end

    it 'responds to error policy' do
      # Always defined, even if config does not specify it
      expect(default_connect).to respond_to(:error_policy)
    end

    it 'calls execute_stmt on the error policy' do
      policy = ::Cequel::Metal::Policy::CassandraError::RetryPolicy.new

      obj = Cequel.connect connect_options.merge(
          cassandra_error_policy: policy)
      expect(policy).to receive(:execute_stmt).at_least(:once)
      obj.execute_with_options(Cequel::Metal::Statement.new('select * from system.peers;'))
    end

    it 'rejects a negative value for retry delay' do
      expect { Cequel.connect connect_options.merge(
        retry_delay: -1.0)
      }.to raise_error(ArgumentError)
    end

    it 'accepts a configured value for retry delay' do
      obj = Cequel.connect connect_options.merge(
        retry_delay: 1337.0)

      # do not compare floats exactly, it is error prone
      # the value is passed to the error policy
      expect(obj.error_policy.retry_delay).to be_within(0.1).of(1337.0)
    end

    it 'can clear active connections' do
      expect {
        default_connect.clear_active_connections!
      }.to change {
        default_connect.client
      }
    end
  end

  describe "#execute" do
    let(:statement) { "SELECT id FROM posts" }
    let(:execution_error) { Cassandra::Errors::OverloadedError.new(1,2,3,4,5,6,7,8,9) }

    context "without a connection error" do
      it "executes a CQL query" do
        expect { cequel.execute(statement) }.not_to raise_error
      end
    end

    context "with a connection error" do
      it "reconnects to cassandra with a new client after no hosts could be reached" do
        allow(cequel.client)
          .to receive(:execute)
               .with(->(s){ s.cql == statement},
                     hash_including(:consistency => cequel.default_consistency))
          .and_raise(Cassandra::Errors::NoHostsAvailable)
          .once

        expect { cequel.execute(statement) }.not_to raise_error
      end

      it "reconnects to cassandra with a new client after execution failed" do
        allow(cequel.client)
          .to receive(:execute)
               .with(->(s){ s.cql == statement},
                     hash_including(:consistency => cequel.default_consistency))
          .and_raise(execution_error)
          .once

        expect { cequel.execute(statement) }.not_to raise_error
      end

      it "reconnects to cassandra with a new client after timeout occurs" do
        allow(cequel.client)
          .to receive(:execute)
               .with(->(s){ s.cql == statement},
                     hash_including(:consistency => cequel.default_consistency))
          .and_raise(Cassandra::Errors::TimeoutError)
          .once

        expect { cequel.execute(statement) }.not_to raise_error
      end
    end
  end

  describe "#prepare_statement" do
    let(:statement) { "SELECT id FROM posts" }
    let(:execution_error) { Cassandra::Errors::OverloadedError.new(1,2,3,4,5,6,7,8,9) }

    context "without a connection error" do
      it "executes a CQL query" do
        expect { cequel.prepare_statement(statement) }.not_to raise_error
      end
    end

    context "with a connection error" do
      it "reconnects to cassandra with a new client after no hosts could be reached" do
        allow(cequel.client)
          .to receive(:prepare)
               .with(->(s){ s == statement})
          .and_raise(Cassandra::Errors::NoHostsAvailable)
          .once

        expect { cequel.prepare_statement(statement) }.not_to raise_error
      end

      it "reconnects to cassandra with a new client after execution failed" do
        allow(cequel.client)
          .to receive(:prepare)
               .with(->(s){ s == statement})
          .and_raise(execution_error)
          .once

        expect { cequel.prepare_statement(statement) }.not_to raise_error
      end
    end
  end
end
