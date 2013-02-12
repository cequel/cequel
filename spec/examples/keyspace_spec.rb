require File.expand_path('../spec_helper', __FILE__)
require 'stringio'
require 'logger'

class CassandraCQL::Thrift::Client::TransportException < Exception
end

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

  describe "#execute" do
    context "when CassandraCQL::Thrift::Client::TransportException is raised" do
      before(:each) do
        @times_called = 0
        @connection = mock('connection')
        @connection.stub(:execute).and_return do
          @times_called += 1
          raise CassandraCQL::Thrift::Client::TransportException if @times_called == 1
        end
      end

      it "logs that a CassandraCQL::Thrift::Client::TransportException exception was raised if logger exists" do
        keyspace = Cequel::Keyspace.new({}) 
        keyspace.class.stub(:logger).and_return(true)
        keyspace.stub(:connection).and_return(@connection)
        keyspace.stub(:log).and_yield
        @connection.stub(:disconnect!)
        logger = mock('logger')
        keyspace.stub(:logger).and_return(logger)
        logger.should_receive(:debug).with("rescued CassandraCQL::Thrift::Client::TransportException, disconnecting and retrying execute") 
        keyspace.execute("SELECT * FROM posts")
      end

      it "disconnects the connection" do
        keyspace = Cequel::Keyspace.new({}) 
        keyspace.stub(:connection).and_return(@connection)
        @connection.should_receive(:disconnect!)
        keyspace.execute("SELECT * FROM posts")
      end

      it "clears the active connection flags" do
        keyspace = Cequel::Keyspace.new({}) 
        @connection.stub(:disconnect!)
        keyspace.stub(:connection).and_return(@connection)
        keyspace.should_receive(:clear_active_connections!)
        keyspace.execute("SELECT * FROM posts")
      end

      it "retries the execute" do
        keyspace = Cequel::Keyspace.new({}) 
        @connection.stub(:disconnect!)
        keyspace.stub(:clear_active_connections!)
        keyspace.stub(:connection).and_return(@connection)
        keyspace.should_receive(:with_connection)
        keyspace.execute("SELECT * FROM posts")
      end
    end
  end
end
