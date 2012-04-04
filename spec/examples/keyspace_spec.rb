require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Keyspace do
  describe '::batch' do
    it 'should send enclosed write statements in bulk' do
      connection.should_receive(:execute).with(<<CQL)
BEGIN BATCH
INSERT INTO posts (id, title) VALUES (1, 'Hey')
UPDATE posts SET body = 'Body'
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
      connection.should_receive(:execute).with(<<CQL)
BEGIN BATCH
INSERT INTO posts (id, title) VALUES (1, 'Hey')
UPDATE posts SET body = 'Body'
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
  end
end
