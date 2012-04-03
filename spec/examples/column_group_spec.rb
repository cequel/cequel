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

  describe '#update' do
    it 'should send basic update statement' do
      connection.should_receive(:execute).
        with 'UPDATE posts SET title = ? AND body = ?', 'Fun times', 'Fun'

      cequel[:posts].update(:title => 'Fun times', :body => 'Fun')
    end

    it 'should send update statement with options' do
      time = Time.now - 10.minutes

      connection.should_receive(:execute).
        with "UPDATE posts USING CONSISTENCY QUORUM AND TTL 600 AND TIMESTAMP #{time.to_i} SET title = ? AND body = ?",
        'Fun times', 'Fun'

      cequel[:posts].update(
        {:title => 'Fun times', :body => 'Fun'},
        :consistency => :quorum, :ttl => 600, :timestamp => time
      )
    end
  end

  describe '#delete' do
    it 'should send basic delete statement' do
      connection.should_receive(:execute).
        with 'DELETE FROM posts'

      cequel[:posts].delete
    end

    it 'should send delete statement for specified columns' do
      connection.should_receive(:execute).
        with 'DELETE title, body FROM posts'

      cequel[:posts].delete(:title, :body)
    end

    it 'should send delete statement with persistence options' do
      time = Time.now - 10.minutes

      connection.should_receive(:execute).
        with "DELETE title, body FROM posts USING CONSISTENCY QUORUM AND TIMESTAMP #{time.to_i}"

      cequel[:posts].delete(
        :title, :body,
        :consistency => :quorum, :timestamp => time
      )
    end
  end
end
