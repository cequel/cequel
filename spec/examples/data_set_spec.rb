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

    it 'should send update statement scoped to current row specifications' do
      connection.should_receive(:execute).
        with "UPDATE posts SET title = 'Fun' WHERE id = 4"

      cequel[:posts].where(:id => 4).update(:title => 'Fun')
    end

    it 'should do nothing if row specification contains empty subquery' do
      connection.stub(:execute).with("SELECT blog_id FROM posts").
        and_return result_stub

      expect do
        cequel[:blogs].where(:id => cequel[:posts].select(:blog_id)).
          update(:title => 'Fun')
      end.to_not raise_error
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

    it 'should send delete statement with scoped row specifications' do
      connection.should_receive(:execute).
        with "DELETE FROM posts WHERE id = 4"

      cequel[:posts].where(:id => 4).delete
    end

    it 'should not do anything if scoped to empty subquery' do
      connection.stub(:execute).with("SELECT blog_id FROM posts").
        and_return result_stub

      expect do
        cequel[:blogs].where(:id => cequel[:posts].select(:blog_id)).
          delete
      end.to_not raise_error
    end
  end

  describe '#truncate' do
    it 'should send a TRUNCATE statement' do
      connection.should_receive(:execute).with("TRUNCATE posts")

      cequel[:posts].truncate
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

    it 'should take a data set as a condition and perform an IN statement' do
      connection.stub(:execute).
        with("SELECT blog_id FROM posts WHERE title = 'Blog'").
        and_return result_stub(
          {:blog_id => 1},
          {:blog_id => 3}
        )

      cequel[:blogs].where(
        :id => cequel[:posts].select(:blog_id).where(:title => 'Blog')
      ).to_cql.
        should == 'SELECT * FROM blogs WHERE id IN (1, 3)'
    end

    it 'should raise EmptySubquery if inner data set has no results' do
      connection.stub(:execute).
        with("SELECT blog_id FROM posts WHERE title = 'Blog'").
        and_return result_stub

      expect do
        cequel[:blogs].where(
          :id => cequel[:posts].select(:blog_id).where(:title => 'Blog')
        ).to_cql
      end.to raise_error(Cequel::EmptySubquery)
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

  describe 'result enumeration' do
    it 'should enumerate over results' do
      connection.stub(:execute).with("SELECT * FROM posts").
        and_return result_stub('id' => 1, 'title' => 'Hey')

      cequel[:posts].to_a.should == [{'id' => 1, 'title' => 'Hey'}]
    end

    it 'should provide results with indifferent access' do
      connection.stub(:execute).with("SELECT * FROM posts").
        and_return result_stub('id' => 1, 'title' => 'Hey')

      cequel[:posts].to_a.first[:id].should == 1
    end

    it 'should not run query if no block given to #each' do
      expect { cequel[:posts].each }.to_not raise_error
    end

    it 'should return Enumerator if no block given to #each' do
      connection.stub(:execute).with("SELECT * FROM posts").
        and_return result_stub('id' => 1, 'title' => 'Hey')

      cequel[:posts].each.each_with_index.map { |row, i| [row[:id], i] }.
        should == [[1, 0]]
    end

    it 'should return no results if subquery is empty' do
      connection.stub(:execute).with("SELECT blog_id FROM posts").
        and_return result_stub

      cequel[:blogs].where(:id => cequel[:posts].select(:blog_id)).to_a.
        should == []
    end
  end

  describe '#first' do
    it 'should run a query with LIMIT 1 and return first row' do
      connection.stub(:execute).with("SELECT * FROM posts LIMIT 1").
        and_return result_stub('id' => 1, 'title' => 'Hey')

      cequel[:posts].first.should == {'id' => 1, 'title' => 'Hey'}
    end

    it 'should return nil if subquery returns empty results' do
      connection.stub(:execute).with("SELECT blog_id FROM posts").
        and_return result_stub

      cequel[:blogs].where(:id => cequel[:posts].select(:blog_id)).first.
        should be_nil
    end
  end

  describe '#count' do
    it 'should run a count query and return count' do
      connection.stub(:execute).with("SELECT COUNT(*) FROM posts").
        and_return result_stub('count' => 4)

      cequel[:posts].count.should == 4
    end

    it 'should return 0 if subquery returns no results' do
      connection.stub(:execute).with("SELECT blog_id FROM posts").
        and_return result_stub

      cequel[:blogs].where(:id => cequel[:posts].select(:blog_id)).count.
        should == 0
    end
  end

end
