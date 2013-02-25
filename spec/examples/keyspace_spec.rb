require File.expand_path('../spec_helper', __FILE__)
require 'stringio'
require 'logger'

describe Cequel::Keyspace do
  describe '::batch' do
    it 'should send enclosed write statements in bulk' do
      connection.should_receive(:execute).with(<<CQL, [:id, :title], [1, 'Hey'], :body, 'Body')
BEGIN BATCH
INSERT INTO posts (?) VALUES (?)
UPDATE posts SET ? = ?
DELETE FROM posts
APPLY BATCH
CQL
      cequel.batch do
        cequel[:posts].insert(:id => 1, :title => 'Hey')
        cequel[:posts].update(:body => 'Body')
        cequel[:posts].delete
      end
    end

    it 'should auto-apply if option given' do
      connection.should_receive(:execute).with(<<CQL, [:id, :title], [1, 'Hey'], :body, 'Body')
BEGIN BATCH
INSERT INTO posts (?) VALUES (?)
UPDATE posts SET ? = ?
APPLY BATCH
CQL
      connection.should_receive(:execute).with(<<CQL)
BEGIN BATCH
DELETE FROM posts
APPLY BATCH
CQL

      cequel.batch(:auto_apply => 2) do
        cequel[:posts].insert(:id => 1, :title => 'Hey')
        cequel[:posts].update(:body => 'Body')
        cequel[:posts].delete
      end
    end

    it 'should do nothing if no statements executed in batch' do
      expect { cequel.batch {} }.to_not raise_error
    end
  end

  describe '::connection_pool' do
    it 'should use connection pool if pool specified' do
      #NOTE: called one time per pool entry
      Cequel::Keyspace.any_instance.should_receive(:build_connection).once.and_return(connection)
      Cequel::Keyspace.any_instance.should_receive(:connection).never
      keyspace = Cequel::Keyspace.new(:pool => 1)

      keyspace.with_connection { |conn| }
      keyspace.with_connection { |conn| }
      keyspace.with_connection { |conn| }
    end

    it 'should not use connection pool if no pool specified' do
      Cequel::Keyspace.any_instance.should_receive(:connection).exactly(3).times.and_return(connection)
      keyspace = Cequel::Keyspace.new({})

      keyspace.with_connection { |conn| }
      keyspace.with_connection { |conn| }
      keyspace.with_connection { |conn| }
    end
  end

  describe '::logger=' do
    let(:io) { StringIO.new }
    let(:logger) { Logger.new(io) }

    before do
      logger.level = Logger::DEBUG
      cequel.logger = logger
    end

    it 'should log queries with bind variables resolved' do
      connection.stub(:execute).with("SELECT ? FROM posts", [:id, :title]).and_return result_stub
      cequel[:posts].select(:id, :title).to_a
      io.string.should =~ /CQL \(\d+ms\) SELECT 'id','title' FROM posts/
    end
  end
end
