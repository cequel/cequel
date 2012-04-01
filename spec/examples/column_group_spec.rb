require File.expand_path('../spec_helper', __FILE__)

describe Cequel::ColumnGroup do
  describe '#insert' do
    it 'should insert a row' do
      connection.should_receive(:execute).
        with 'INSERT INTO posts (id, title) VALUES (?, ?)', 1, 'Fun times'

      cequel[:posts].insert(:id => 1, :title => 'Fun times')
    end

    it 'should include consistency argument' do
      connection.should_receive(:execute).
        with 'INSERT INTO posts (id, title) VALUES (?, ?) USING CONSISTENCY QUORUM', 1, 'Fun times'

      cequel[:posts].insert(
        {:id => 1, :title => 'Fun times'},
        :consistency => :quorum
      )
    end

    it 'should include ttl argument' do
      connection.should_receive(:execute).
        with 'INSERT INTO posts (id, title) VALUES (?, ?) USING TTL 600', 1, 'Fun times'

      cequel[:posts].insert(
        {:id => 1, :title => 'Fun times'},
        :ttl => 10.minutes
      )
    end

    it 'should include timestamp argument' do
      time = Time.now - 10.minutes
      connection.should_receive(:execute).
        with "INSERT INTO posts (id, title) VALUES (?, ?) USING TIMESTAMP #{time.to_i}", 1, 'Fun times'

      cequel[:posts].insert(
        {:id => 1, :title => 'Fun times'},
        :timestamp => time
      )
    end

    it 'should include multiple arguments joined by AND' do
      time = Time.now - 10.minutes
      connection.should_receive(:execute).
        with "INSERT INTO posts (id, title) VALUES (?, ?) USING CONSISTENCY QUORUM AND TTL 600 AND TIMESTAMP #{time.to_i}", 1, 'Fun times'

      cequel[:posts].insert(
        {:id => 1, :title => 'Fun times'},
        :consistency => :quorum,
        :ttl => 600,
        :timestamp => time
      )
    end
  end
end
