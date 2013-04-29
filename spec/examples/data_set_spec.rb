require File.expand_path('../spec_helper', __FILE__)

describe Cequel::DataSet do
  describe '#insert' do
    it 'should insert a row' do
      connection.should_receive(:execute).
        with "INSERT INTO posts (?) VALUES (?)", [:id, :title], [1, 'Fun times']

      cequel[:posts].insert(:id => 1, :title => 'Fun times')
    end

    it 'should include consistency argument' do
      connection.should_receive(:execute).
        with "INSERT INTO posts (?) VALUES (?) USING CONSISTENCY QUORUM", [:id, :title], [1, 'Fun times']

      cequel[:posts].insert(
        {:id => 1, :title => 'Fun times'},
        :consistency => :quorum
      )
    end

    it 'should respect with_consistency block' do
      connection.should_receive(:execute).
        with "INSERT INTO posts (?) VALUES (?) USING CONSISTENCY QUORUM", [:id, :title], [1, 'Fun times']

      cequel.with_consistency(:quorum) do
        cequel[:posts].insert(:id => 1, :title => 'Fun times')
      end
    end

    it 'should include ttl argument' do
      connection.should_receive(:execute).
        with "INSERT INTO posts (?) VALUES (?) USING TTL 600", [:id, :title], [1, 'Fun times']

      cequel[:posts].insert(
        {:id => 1, :title => 'Fun times'},
        :ttl => 10.minutes
      )
    end

    it 'should include timestamp argument' do
      time = Time.now - 10.minutes
      connection.should_receive(:execute).
        with "INSERT INTO posts (?) VALUES (?) USING TIMESTAMP #{time.to_i}", [:id, :title], [1, 'Fun times']

      cequel[:posts].insert(
        {:id => 1, :title => 'Fun times'},
        :timestamp => time
      )
    end

    it 'should include multiple arguments joined by AND' do
      time = Time.now - 10.minutes
      connection.should_receive(:execute).
        with "INSERT INTO posts (?) VALUES (?) USING CONSISTENCY QUORUM AND TTL 600 AND TIMESTAMP #{time.to_i}",
        [:id, :title], [1, 'Fun times']

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
        with "UPDATE posts SET ? = ?, ? = ?", :title, 'Fun times', :body, 'Fun'

      cequel[:posts].update(:title => 'Fun times', :body => 'Fun')
    end

    it 'should send update statement with options' do
      time = Time.now - 10.minutes

      connection.should_receive(:execute).
        with "UPDATE posts USING CONSISTENCY QUORUM AND TTL 600 AND TIMESTAMP #{time.to_i} SET ? = ?, ? = ?", :title, 'Fun times', :body, 'Fun'

      cequel[:posts].update(
        {:title => 'Fun times', :body => 'Fun'},
        :consistency => :quorum, :ttl => 600, :timestamp => time
      )
    end

    it 'should respect default consistency' do
      connection.should_receive(:execute).
        with "UPDATE posts USING CONSISTENCY QUORUM SET ? = ?, ? = ?", :title, 'Fun times', :body, 'Fun'

      cequel.with_consistency(:quorum) do
        cequel[:posts].update(:title => 'Fun times', :body => 'Fun')
      end
    end

    it 'should send update statement scoped to current row specifications' do
      connection.should_receive(:execute).
        with "UPDATE posts SET ? = ? WHERE ? = ?", :title, 'Fun', :id, 4

      cequel[:posts].where(:id => 4).update(:title => 'Fun')
    end

    it 'should do nothing if row specification contains empty subquery' do
      connection.stub(:execute).with("SELECT ? FROM posts", [:blog_id]).
        and_return result_stub

      expect do
        cequel[:blogs].where(:id => cequel[:posts].select(:blog_id)).
          update(:title => 'Fun')
      end.to_not raise_error
    end
  end

  describe '#increment' do
    it 'should increment counter columns' do
      connection.should_receive(:execute).with(
        'UPDATE comment_counts SET ? = ? + ?, ? = ? + ? WHERE ? = ?',
        'somepost', 'somepost', 1,
        'anotherpost', 'anotherpost', 2,
        'blog_id', 'myblog'
      )
      cequel[:comment_counts].where('blog_id' => 'myblog').
        increment('somepost' => 1, 'anotherpost' => 2)
    end
  end

  describe '#decrement' do
    it 'should decrement counter columns' do
      connection.should_receive(:execute).with(
        'UPDATE comment_counts SET ? = ? - ?, ? = ? - ? WHERE ? = ?',
        'somepost', 'somepost', 1,
        'anotherpost', 'anotherpost', 2,
        'blog_id', 'myblog'
      )
      cequel[:comment_counts].where('blog_id' => 'myblog').
        decrement('somepost' => 1, 'anotherpost' => 2)
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
        with 'DELETE ? FROM posts', [:title, :body]

      cequel[:posts].delete(:title, :body)
    end

    it 'should send delete statement with persistence options' do
      time = Time.now - 10.minutes

      connection.should_receive(:execute).
        with "DELETE ? FROM posts USING CONSISTENCY QUORUM AND TIMESTAMP #{time.to_i}", [:title, :body]

      cequel[:posts].delete(
        :title, :body,
        :consistency => :quorum, :timestamp => time
      )
    end

    it 'should respect default consistency' do
      connection.should_receive(:execute).
        with "DELETE ? FROM posts USING CONSISTENCY QUORUM", [:title, :body]

      cequel.with_consistency(:quorum) do
        cequel[:posts].delete(:title, :body)
      end
    end

    it 'should send delete statement with scoped row specifications' do
      connection.should_receive(:execute).
        with "DELETE FROM posts WHERE ? = ?", :id, 4

      cequel[:posts].where(:id => 4).delete
    end

    it 'should not do anything if scoped to empty subquery' do
      connection.stub(:execute).with("SELECT ? FROM posts", [:blog_id]).
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

  describe '#cql' do
    it 'should generate select statement with all columns' do
      cequel[:posts].cql.should == ['SELECT * FROM posts']
    end
  end

  describe '#select' do
    it 'should generate select statement with given columns' do
      cequel[:posts].select(:id, :title).cql.
        should == ['SELECT ? FROM posts', [:id, :title]]
    end

    it 'should accept array argument' do
      cequel[:posts].select([:id, :title]).cql.
        should == ['SELECT ? FROM posts', [:id, :title]]
    end

    it 'should combine multiple selects' do
      cequel[:posts].select(:id).select(:title).cql.
        should == ['SELECT ? FROM posts', [:id, :title]]
    end

    it 'should accept :first option' do
      cequel[:posts].select(:first => 100).cql.
        should == ['SELECT FIRST 100 * FROM posts']
    end

    it 'should accept :last option' do
      cequel[:posts].select(:last => 100).cql.
        should == ['SELECT FIRST 100 REVERSED * FROM posts']
    end

    it 'should accept column range' do
      cequel[:posts].select(1..10).cql.
        should == ['SELECT ?..? FROM posts', 1, 10]
    end

    it 'should accept :from option' do
      cequel[:posts].select(:from => 10).cql.
        should == ['SELECT ?..? FROM posts', 10, '']
    end

    it 'should combine range and column limit options' do
      cequel[:posts].select(:first => 100, :from => 10).cql.
        should == ['SELECT FIRST 100 ?..? FROM posts', 10, '']
    end

    it 'should chain select options' do
      cequel[:posts].select(:first => 100).select(:from => 10).cql.
        should == ['SELECT FIRST 100 ?..? FROM posts', 10, '']
    end
  end

  describe '#select!' do
    it 'should generate select statement with given columns' do
      cequel[:posts].select(:id, :title).select!(:published).cql.
        should == ['SELECT ? FROM posts', [:published]]
    end
  end

  describe '#where' do
    it 'should build WHERE statement from hash' do
      cequel[:posts].where(:title => 'Hey').cql.
        should == ["SELECT * FROM posts WHERE ? = ?", :title, 'Hey']
    end

    it 'should build WHERE statement from multi-element hash' do
      cequel[:posts].where(:title => 'Hey', :body => 'Guy').cql.
        should == ["SELECT * FROM posts WHERE ? = ? AND ? = ?", :title, 'Hey', :body, 'Guy']
    end

    it 'should build WHERE statement with IN' do
      cequel[:posts].where(:id => [1, 2, 3, 4]).cql.
        should == ['SELECT * FROM posts WHERE ? IN (?)', :id, [1, 2, 3, 4]]
    end

    it 'should use = if provided one-element array' do
      cequel[:posts].where(:id => [1]).cql.
        should == ['SELECT * FROM posts WHERE ? = ?', :id, 1]
    end

    it 'should build WHERE statement from CQL string' do
      cequel[:posts].where("title = ?", 'Hey').cql.
        should == ["SELECT * FROM posts WHERE title = ?", 'Hey']
    end

    it 'should build WHERE statement from CQL string with bind variables' do
      cequel[:posts].where("title = ?", 'Hey').cql.
        should == ["SELECT * FROM posts WHERE title = ?", 'Hey']
    end

    it 'should aggregate multiple WHERE statements' do
      cequel[:posts].where(:title => 'Hey').where('body = ?', 'Sup').cql.
        should == ["SELECT * FROM posts WHERE ? = ? AND body = ?", :title, 'Hey', 'Sup']
    end

    it 'should take a data set as a condition and perform an IN statement' do
      connection.stub(:execute).
        with("SELECT ? FROM posts WHERE ? = ?", [:blog_id], :title, 'Blog').
        and_return result_stub(
          {:blog_id => 1},
          {:blog_id => 3}
        )

      cequel[:blogs].where(
        :id => cequel[:posts].select(:blog_id).where(:title => 'Blog')
      ).cql.
        should == ['SELECT * FROM blogs WHERE ? IN (?)', :id, [1, 3]]
    end

    it 'should raise EmptySubquery if inner data set has no results' do
      connection.stub(:execute).
        with("SELECT ? FROM posts WHERE ? = ?", [:blog_id], :title, 'Blog').
        and_return result_stub

      expect do
        cequel[:blogs].where(
          :id => cequel[:posts].select(:blog_id).where(:title => 'Blog')
        ).cql
      end.to raise_error(Cequel::EmptySubquery)
    end

  end

  describe '#where!' do
    it 'should override chained conditions' do
      cequel[:posts].where(:title => 'Hey').where!(:title => 'Cequel').cql.
        should == ["SELECT * FROM posts WHERE ? = ?", :title, 'Cequel']
    end
  end

  describe '#consistency' do
    it 'should add USING CONSISTENCY to select' do
      cequel[:posts].consistency(:quorum).cql.
        should == ["SELECT * FROM posts USING CONSISTENCY QUORUM"]
    end
  end

  describe 'in with_consistency block' do
    it 'should use default consistency' do
      cequel.with_consistency(:quorum) do
        cequel[:posts].cql.
          should == ["SELECT * FROM posts USING CONSISTENCY QUORUM"]
      end
    end
  end

  describe '#limit' do
    it 'should add LIMIT' do
      cequel[:posts].limit(2).cql.
        should == ['SELECT * FROM posts LIMIT 2']
    end
  end

  describe 'chaining scopes' do
    it 'should aggregate all scope options' do
      cequel[:posts].
        select(:id, :title).
        consistency(:quorum).
        where(:title => 'Hey').
        limit(3).cql.
        should == ["SELECT ? FROM posts USING CONSISTENCY QUORUM WHERE ? = ? LIMIT 3", [:id, :title], :title, 'Hey']
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
      connection.stub(:execute).with("SELECT ? FROM posts", [:blog_id]).
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
      connection.stub(:execute).with("SELECT ? FROM posts", [:blog_id]).
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
      connection.stub(:execute).with("SELECT ? FROM posts", [:blog_id]).
        and_return result_stub

      cequel[:blogs].where(:id => cequel[:posts].select(:blog_id)).count.
        should == 0
    end

    it 'should use limit if specified' do
      connection.stub(:execute).with("SELECT COUNT(*) FROM posts LIMIT 100000").
        and_return result_stub('count' => 4)

      cequel[:posts].limit(100_000).count.should == 4
    end
  end

end
