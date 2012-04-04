require File.expand_path('../spec_helper', __FILE__)

describe Cequel::DataSet do
  describe '#insert' do
    it 'should insert a row' do
      connection.should_receive(:execute).
        with "INSERT INTO posts (id, title) VALUES (1, 'Fun times')"

      cequel[:posts].insert(:id => 1, :title => 'Fun times')
    end

    it 'should include consistency argument' do
      connection.should_receive(:execute).
        with "INSERT INTO posts (id, title) VALUES (1, 'Fun times') USING CONSISTENCY QUORUM"

      cequel[:posts].insert(
        {:id => 1, :title => 'Fun times'},
        :consistency => :quorum
      )
    end

    it 'should include ttl argument' do
      connection.should_receive(:execute).
        with "INSERT INTO posts (id, title) VALUES (1, 'Fun times') USING TTL 600"

      cequel[:posts].insert(
        {:id => 1, :title => 'Fun times'},
        :ttl => 10.minutes
      )
    end

    it 'should include timestamp argument' do
      time = Time.now - 10.minutes
      connection.should_receive(:execute).
        with "INSERT INTO posts (id, title) VALUES (1, 'Fun times') USING TIMESTAMP #{time.to_i}"

      cequel[:posts].insert(
        {:id => 1, :title => 'Fun times'},
        :timestamp => time
      )
    end

    it 'should include multiple arguments joined by AND' do
      time = Time.now - 10.minutes
      connection.should_receive(:execute).
        with "INSERT INTO posts (id, title) VALUES (1, 'Fun times') USING CONSISTENCY QUORUM AND TTL 600 AND TIMESTAMP #{time.to_i}"

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
        with "UPDATE posts SET title = 'Fun times' AND body = 'Fun'"

      cequel[:posts].update(:title => 'Fun times', :body => 'Fun')
    end

    it 'should send update statement with options' do
      time = Time.now - 10.minutes

      connection.should_receive(:execute).
        with "UPDATE posts USING CONSISTENCY QUORUM AND TTL 600 AND TIMESTAMP #{time.to_i} SET title = 'Fun times' AND body = 'Fun'"

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

  describe '#to_cql' do
    it 'should generate select statement with all columns' do
      cequel[:posts].to_cql.should == 'SELECT * FROM posts'
    end
  end

  describe '#select' do
    it 'should generate select statement with given columns' do
      cequel[:posts].select(:id, :title).to_cql.
        should == 'SELECT id, title FROM posts'
    end

    it 'should accept array argument' do
      cequel[:posts].select([:id, :title]).to_cql.
        should == 'SELECT id, title FROM posts'
    end

    it 'should combine multiple selects' do
      cequel[:posts].select(:id).select(:title).to_cql.
        should == 'SELECT id, title FROM posts'
    end
  end

  describe '#where' do
    it 'should build WHERE statement from hash' do
      cequel[:posts].where(:title => 'Hey').to_cql.
        should == "SELECT * FROM posts WHERE title = 'Hey'"
    end

    it 'should build WHERE statement from multi-element hash' do
      cequel[:posts].where(:title => 'Hey', :body => 'Guy').to_cql.
        should == "SELECT * FROM posts WHERE title = 'Hey' AND body = 'Guy'"
    end

    it 'should build WHERE statement with IN' do
      cequel[:posts].where(:id => [1, 2, 3, 4]).to_cql.
        should == 'SELECT * FROM posts WHERE id IN (1, 2, 3, 4)'
    end

    it 'should build WHERE statement from CQL string' do
      cequel[:posts].where("title = 'Hey'").to_cql.
        should == "SELECT * FROM posts WHERE title = 'Hey'"
    end

    it 'should build WHERE statement from CQL string with bind variables' do
      cequel[:posts].where("title = ?", 'Hey').to_cql.
        should == "SELECT * FROM posts WHERE title = 'Hey'"
    end

    it 'should aggregate multiple WHERE statements' do
      cequel[:posts].where(:title => 'Hey').where('body = ?', 'Sup').to_cql.
        should == "SELECT * FROM posts WHERE title = 'Hey' AND body = 'Sup'"
    end
  end

  describe '#consistency' do
    it 'should add USING CONSISTENCY to select' do
      cequel[:posts].consistency(:quorum).to_cql.
        should == "SELECT * FROM posts USING CONSISTENCY QUORUM"
    end
  end

  describe '#limit' do
    it 'should add LIMIT' do
      cequel[:posts].limit(2).to_cql.
        should == 'SELECT * FROM posts LIMIT 2'
    end
  end

  describe 'chaining scopes' do
    it 'should aggregate all scope options' do
      cequel[:posts].
        select(:id, :title).
        consistency(:quorum).
        where(:title => 'Hey').
        limit(3).to_cql.
        should == "SELECT id, title FROM posts USING CONSISTENCY QUORUM WHERE title = 'Hey' LIMIT 3"
    end
  end
end
