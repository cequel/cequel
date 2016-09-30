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
    it "is true for existent keyspaces" do
      expect(cequel.exists?).to eq true
    end

    it "is false for non-existent keyspaces" do
      nonexistent_keyspace = Cequel.connect host: Cequel::SpecSupport::Helpers.host,
                           port: Cequel::SpecSupport::Helpers.port,
                           keyspace: "totallymadeup"

      expect(nonexistent_keyspace.exists?).to be false
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

  describe "#execute" do
    let(:statement) { "SELECT id FROM posts" }

    context "without a connection error" do
      it "executes a CQL query" do
        expect { cequel.execute(statement) }.not_to raise_error
      end
    end

    context "with a connection error" do
      it "reconnects to cassandra with a new client after first failed connection" do
        allow(cequel.client)
          .to receive(:execute)
               .with(->(s){ s.cql == statement},
                     hash_including(:consistency => cequel.default_consistency))
          .and_raise(Ione::Io::ConnectionError)
          .once

        expect { cequel.execute(statement) }.not_to raise_error
      end
    end
  end
end
