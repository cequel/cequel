require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Scope do
  describe '#each' do
    it 'should provide enumerator for query results' do
      connection.stub(:execute).with("SELECT * FROM posts").
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.all.map { |post| [post.id, post.title] }.
        should == [[1, 'Cequel']]
    end

    it 'should enumerate results for just id if no key restriction' do
      connection.stub(:execute).with("SELECT ? FROM posts", [:id]).
        and_return result_stub(:id => 1)

      Post.select(:id).to_a.map { |post| post.id }.should == [1]
    end

    it 'should not enumerate results for just id if key restriction' do
      connection.stub(:execute).with("SELECT * FROM posts").
        and_return result_stub(:id => 1)

      Post.all.to_a.map { |post| post.id }.should == []
    end

    it 'should provide enumerator if no block given' do
      enum = Post.all.each

      connection.stub(:execute).with("SELECT * FROM posts").
        and_return result_stub(:id => 1, :title => 'Cequel')

      enum.map { |post| post.title }.should == ['Cequel']
    end

    it 'should enumerate zero times if empty-collection key restriction given' do
      Post.where(:id => []).to_a.should == []
    end

    it 'should enumerate zero times if empty-collection restriction given' do
      Post.where(:title => []).to_a.should == []
    end
  end

  describe '#first' do
    it 'should query for a single post' do
      connection.stub(:execute).with("SELECT * FROM posts LIMIT 1").
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.first.title.should == 'Cequel'
    end

    it 'should query for a single post within scope' do
      connection.stub(:execute).with("SELECT ? FROM posts LIMIT 1", [:id, :title]).
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.select(:id, :title).first.title.should == 'Cequel'
    end

    it 'should query scopes successively when multi-valued non-key column selected' do
      connection.stub(:execute).with("SELECT * FROM posts WHERE ? = ? LIMIT 1", :title, 'Cequel').
        and_return result_stub
      connection.stub(:execute).with("SELECT * FROM posts WHERE ? = ? LIMIT 1", :title, 'Cassandra').
        and_return result_stub(:id => 1, :title => 'Cassandra')
      connection.stub(:execute).with("SELECT * FROM posts WHERE ? = ? LIMIT 1", :title, 'CQL').
        and_return result_stub(:id => 2, :title => 'CQL')

      Post.where(:title => %w(Cequel Cassandra CQL)).first.title.
        should == 'Cassandra'
    end

    it 'should apply index preference when specified' do
      connection.should_receive(:execute).
        with("SELECT * FROM assets WHERE ? = ? AND ? = ? LIMIT 1", :checksum, 'abcdef', :class_name, 'Photo').
        and_return result_stub
      Photo.where(:checksum => 'abcdef').first
    end

    it 'should return nil when empty key collection given' do
      Post.where(:id => []).first.should be_nil
    end

    it 'should return nil when empty non-key collection given' do
      Post.where(:title => []).first.should be_nil
    end
  end

  describe '#count' do
    it 'should count records' do
      connection.stub(:execute).with("SELECT COUNT(*) FROM posts").
        and_return result_stub('count' => 5)

      Post.count.should == 5
    end

    it 'should count records in scope' do
      connection.stub(:execute).
        with("SELECT COUNT(*) FROM posts WHERE ? = ?", :blog_id, 1).
        and_return result_stub('count' => 5)

      Post.where(:blog_id => 1).count.should == 5
    end

    it 'should raise error if attempting count with key restriction' do
      expect { Post.where(:id => [1, 2, 3]).count }.
        to raise_error(Cequel::Model::InvalidQuery)
    end

    it 'should perform multiple COUNT queries if non-key column selected for multiple values' do
      connection.stub(:execute).
        with("SELECT COUNT(*) FROM posts WHERE ? = ?", :blog_id, 1).
        and_return result_stub('count' => 3)
      connection.stub(:execute).
        with("SELECT COUNT(*) FROM posts WHERE ? = ?", :blog_id, 2).
        and_return result_stub('count' => 2)

      Post.where(:blog_id => [1, 2]).count.should == 5
    end

    it 'should return nil if empty non-key restriction given' do
      Post.where(:title => []).count.should == 0
    end

    it 'should apply index preference when specified' do
      connection.should_receive(:execute).
        with("SELECT COUNT(*) FROM assets WHERE ? = ? AND ? = ?", :checksum, 'abcdef', :class_name, 'Photo').
        and_return result_stub('count' => 0)
      Photo.where(:checksum => 'abcdef').count
    end
  end

  describe '#find' do
    it 'should send scoped query when no block is passed' do
      connection.stub(:execute).
        with("SELECT ? FROM posts WHERE ? = ? LIMIT 1", [:id, :title], :id, 1).
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.select(:id, :title).find(1).title.should == 'Cequel'
    end

    it 'should send scoped query with multiple keys when no block is passed' do
      connection.stub(:execute).
        with("SELECT ? FROM posts WHERE ? IN (?)", [:id, :title], :id, [1, 2]).
        and_return result_stub(
          {:id => 1, :title => 'Cequel'},
          {:id => 2, :title => 'Cequel 2'}
        )

      Post.select(:id, :title).find(1, 2).
        map { |post| post.title }.should == ['Cequel', 'Cequel 2']
    end

    it 'should delegate to enumerator when block is passed' do
      connection.stub(:execute).
        with("SELECT ? FROM posts", [:id, :title]).
        and_return result_stub(
          {:id => 1, :title => 'Cequel'},
          {:id => 2, :title => 'Cequel 2'}
        )

      Post.select(:id, :title).find { |post| post.id == 2 }.title.
        should == 'Cequel 2'
    end
  end

  describe '#any?' do
    it 'if called without block, should return true if COUNT > 0' do
      connection.stub(:execute).
        with("SELECT COUNT(*) FROM posts WHERE ? = ?", :blog_id, 1).
        and_return result_stub('count' => 5)

      Post.where(:blog_id => 1).any?.should be_true
    end

    it 'if called without block, should return false if COUNT == 0' do
      connection.stub(:execute).
        with("SELECT COUNT(*) FROM posts WHERE ? = ?", :blog_id, 1).
        and_return result_stub('count' => 0)

      Post.where(:blog_id => 1).any?.should be_false
    end

    it 'if called with block should delegate to enumerator' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ?", :blog_id, 1).
        and_return result_stub(
          {:id => 1, :title => 'Cequel'},
          {:id => 2, :title => 'Cequel 2'}
        )

      Post.where(:blog_id => 1).any? { |post| post.id == 1 }.should be_true
      Post.where(:blog_id => 1).any? { |post| post.id == 8 }.should be_false
    end
  end

  describe '#none?' do
    it 'if called without block, should return false if COUNT > 0' do
      connection.stub(:execute).
        with("SELECT COUNT(*) FROM posts WHERE ? = ?", :blog_id, 1).
        and_return result_stub('count' => 5)

      Post.where(:blog_id => 1).none?.should be_false
    end

    it 'if called without block, should return true if COUNT == 0' do
      connection.stub(:execute).
        with("SELECT COUNT(*) FROM posts WHERE ? = ?", :blog_id, 1).
        and_return result_stub('count' => 0)

      Post.where(:blog_id => 1).none?.should be_true
    end

    it 'if called with block should delegate to enumerator' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ?", :blog_id, 1).
        and_return result_stub(
          {:id => 1, :title => 'Cequel'},
          {:id => 2, :title => 'Cequel 2'}
        )

      Post.where(:blog_id => 1).none? { |post| post.id == 1 }.should be_false
      Post.where(:blog_id => 1).none? { |post| post.id == 8 }.should be_true
    end
  end

  describe '#one?' do
    it 'if called without block, should return false if COUNT == 0' do
      connection.stub(:execute).
        with("SELECT COUNT(*) FROM posts WHERE ? = ?", :blog_id, 1).
        and_return result_stub('count' => 0)

      Post.where(:blog_id => 1).one?.should be_false
    end

    it 'if called without block, should return true if COUNT == 1' do
      connection.stub(:execute).
        with("SELECT COUNT(*) FROM posts WHERE ? = ?", :blog_id, 1).
        and_return result_stub('count' => 1)

      Post.where(:blog_id => 1).one?.should be_true
    end

    it 'if called without block, should return false if COUNT > 1' do
      connection.stub(:execute).
        with("SELECT COUNT(*) FROM posts WHERE ? = ?", :blog_id, 1).
        and_return result_stub('count' => 12)

      Post.where(:blog_id => 1).one?.should be_false
    end

    it 'if called with block should delegate to enumerator' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ?", :blog_id, 1).
        and_return result_stub(
          {:id => 1, :title => 'Cequel'},
          {:id => 2, :title => 'Cequel 2'}
        )

      Post.where(:blog_id => 1).none? { |post| post.id == 1 }.should be_false
      Post.where(:blog_id => 1).none? { |post| post.id == 8 }.should be_true
    end
  end

  describe '#find_in_batches' do
    it 'should select in batches of given size' do
      connection.stub(:execute).with("SELECT * FROM posts LIMIT 2").
        and_return result_stub(
          {:id => 1, :title => 'Post 1'},
          {:id => 2, :title => 'Post 2'}
        )
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? > ? LIMIT 2", :id, 2).
        and_return result_stub(:id => 3, :title => 'Post 3')
      batches = []
      Post.find_in_batches(:batch_size => 2) do |batch|
        batches << batch
      end
      batches.map { |batch| batch.map(&:title) }.should ==
        [['Post 1', 'Post 2'], ['Post 3']]
    end

    it 'should not duplicate last key if given back first in next batch' do
      connection.stub(:execute).with("SELECT * FROM posts LIMIT 2").
        and_return result_stub(
          {:id => 1, :title => 'Post 1'},
          {:id => 2, :title => 'Post 2'}
        )
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? > ? LIMIT 2", :id, 2).
        and_return result_stub(
          {:id => 2, :title => 'Post 2'},
          {:id => 3, :title => 'Post 3'}
        )
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? > ? LIMIT 2", :id, 3).
        and_return result_stub()
      batches = []
      Post.find_in_batches(:batch_size => 2) do |batch|
        batches << batch
      end
      batches.map { |batch| batch.map(&:title) }.should ==
        [['Post 1', 'Post 2'], ['Post 3']]
    end

    it 'should iterate over batches of keys' do
      connection.stub(:execute).with("SELECT * FROM posts WHERE ? IN (?)", :id, [1, 2]).
        and_return result_stub(
          {:id => 1, :title => 'Post 1'},
          {:id => 2, :title => 'Post 2'}
        )
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ?", :id, 3).
        and_return result_stub(:id => 3, :title => 'Post 3')
      batches = []
      Post.where(:id => [1, 2, 3]).find_in_batches(:batch_size => 2) do |batch|
        batches << batch
      end
      batches.map { |batch| batch.map(&:title) }.should ==
        [['Post 1', 'Post 2'], ['Post 3']]
    end

    it 'should respect scope' do
      connection.stub(:execute).with("SELECT ? FROM posts LIMIT 2", [:id, :title]).
        and_return result_stub(
          {:id => 1, :title => 'Post 1'},
          {:id => 2, :title => 'Post 2'}
        )
      connection.stub(:execute).
        with("SELECT ? FROM posts WHERE ? > ? LIMIT 2", [:id, :title], :id, 2).
        and_return result_stub(:id => 3, :title => 'Post 3')
      batches = []
      Post.select(:id, :title).find_in_batches(:batch_size => 2) do |batch|
        batches << batch
      end
      batches.map { |batch| batch.map(&:title) }.should ==
        [['Post 1', 'Post 2'], ['Post 3']]
    end

    it 'should add key column to SELECT if omitted' do
      connection.stub(:execute).with("SELECT ? FROM posts LIMIT 2", [:title, :id]).
        and_return result_stub(
          {:id => 1, :title => 'Post 1'},
          {:id => 2, :title => 'Post 2'}
        )
      connection.stub(:execute).
        with("SELECT ? FROM posts WHERE ? > ? LIMIT 2", [:title, :id], :id, 2).
        and_return result_stub(:id => 3, :title => 'Post 3')
      batches = []
      Post.select(:title).find_in_batches(:batch_size => 2) do |batch|
        batches << batch
      end
      batches.map { |batch| batch.map(&:title) }.should ==
        [['Post 1', 'Post 2'], ['Post 3']]
    end
  end

  describe '#find_each' do
    it 'should iterate over batches and yield results one by one' do
      connection.stub(:execute).with("SELECT * FROM posts LIMIT 2").
        and_return result_stub(
          {:id => 1, :title => 'Post 1'},
          {:id => 2, :title => 'Post 2'}
        )
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? > ? LIMIT 2", :id, 2).
        and_return result_stub(:id => 3, :title => 'Post 3')
      Post.find_each(:batch_size => 2).map { |post| post.title }.
        should == ['Post 1', 'Post 2', 'Post 3']
    end
  end

  describe '#select' do
    it 'should scope columns in data set' do
      connection.stub(:execute).with("SELECT ? FROM posts", [:id, :title]).
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.select(:id, :title).map { |post| post.title }.should == ['Cequel']
    end

    it 'should fail fast if attempting to select only key column with restrictions on key column' do
      expect { Post.where(:id => 1).select(:id) }.
        to raise_error(Cequel::Model::InvalidQuery)
    end

    it 'should delegate to enumerator if block given' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ?", :blog_id, 1).
        and_return result_stub(
          {:id => 1, :title => 'Cequel'},
          {:id => 2, :title => 'Cequel 2'},
          {:id => 3, :title => 'Cequel 3'}
        )

      Post.where(:blog_id => 1).select { |post| post.id < 3 }.
        map { |post| post.title }.should == ['Cequel', 'Cequel 2']
    end
  end

  describe '#select!' do
    it 'should override previous columns in data set' do
      connection.stub(:execute).with("SELECT ? FROM posts", [:id, :published]).
        and_return result_stub(:id => 1, :published => true)

      Post.select(:id, :title).select!(:id, :published).
        map { |post| post.published }.should == [true]
    end
  end

  describe '#consistency' do
    it 'should scope consistency in data set' do
      connection.stub(:execute).with("SELECT * FROM posts USING CONSISTENCY QUORUM").
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.consistency(:quorum).map { |post| post.title }.should == ['Cequel']
    end
  end

  describe '#where' do
    it 'should scope to row specifications in data set' do
      connection.stub(:execute).with("SELECT * FROM posts WHERE ? = ?", :id, 1).
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.where(:id => 1).map { |post| post.title }.should == ['Cequel']
    end

    it 'should perform multiple queries if IN query performed on non-key column' do
      connection.stub(:execute).with("SELECT * FROM posts WHERE ? = ?", :title, 'Cequel').
        and_return result_stub(:id => 1, :title => 'Cequel')
      connection.stub(:execute).with("SELECT * FROM posts WHERE ? = ?", :title, 'Fun').
        and_return result_stub(
          {:id => 2, :title => 'Fun'},
          {:id => 3, :title => 'Fun'}
        )
      Post.where(:title => %w(Cequel Fun)).map(&:id).
        should == [1, 2, 3]
    end

    it 'should fail fast if attempting to select only key column with restrictions on key column' do
      expect { Post.select(:id).where(:id => 1) }.
        to raise_error(Cequel::Model::InvalidQuery)
    end

    it 'should fail fast if attempting to mix key and non-key columns' do
      expect { Post.where(:id => 1).where(:title => 'Cequel') }.
        to raise_error(Cequel::Model::InvalidQuery)
    end

    it 'should use index preference if given' do
      connection.should_receive(:execute).
        with("SELECT * FROM assets WHERE ? = ? AND ? = ?", :checksum, 'abcdef', :class_name, 'Photo').
        and_return result_stub
      Photo.where(:checksum => 'abcdef').to_a
    end
  end

  describe '#where!' do
    it 'should override previously chained row specifications' do
      connection.stub(:execute).with("SELECT * FROM posts WHERE ? = ?", :title, 'Cequel').
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.where(:id => 1).where!(:title => 'Cequel').
        map { |post| post.title }.should == ['Cequel']
    end
  end

  describe '#limit' do
    it 'should limit results in data set' do
      connection.stub(:execute).with("SELECT * FROM posts LIMIT 5").
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.limit(5).map { |post| post.title }.should == ['Cequel']
    end
  end

  describe 'chaining' do
    it 'should aggregate scopes' do
      connection.stub(:execute).
        with("SELECT ? FROM posts USING CONSISTENCY QUORUM WHERE ? = ? LIMIT 5", [:id, :title], :blog_id, 1).
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.select(:id, :title).
        consistency(:quorum).
        where(:blog_id => 1).
        limit(5).
        map { |post| post.title }.should == ['Cequel']
    end

    it 'should delegate unknown methods to the underlying class with self as current scope' do
      connection.stub(:execute).
        with("SELECT ? FROM posts USING CONSISTENCY QUORUM WHERE ? = ? LIMIT 5", [:id, :title], :blog_id, 1).
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.select(:id, :title).
        consistency(:quorum).
        for_blog(1).
        limit(5).
        map { |post| post.title }.should == ['Cequel']
    end

  end

  describe '#update_all' do
    context 'with no scope restrictions' do
      let(:scope) { Post }

      it 'should get all keys and then update htem' do
        connection.should_receive(:execute).
          with("SELECT ? FROM posts", [:id]).
          and_return result_stub(
            {:id => 1},
            {:id => 2}
          )
        connection.should_receive(:execute).
          with "UPDATE posts SET ? = ? WHERE ? IN (?)", :title, 'Cequel', :id, [1, 2]
        scope.update_all(:title => 'Cequel')
      end
    end

    context 'with scope selecting on ids' do
      let(:scope) { Post.where(:id => [1, 2]) }

      it 'should issue scoped update request' do
        connection.should_receive(:execute).
          with "UPDATE posts SET ? = ? WHERE ? IN (?)", :title, 'Cequel', :id, [1, 2]
        scope.update_all(:title => 'Cequel')
      end

    end

    context 'with scope selecting on non-IDs' do
      let(:scope) { Post.where(:published => false) }

      it 'should perform "subquery" and issue update' do
        connection.stub(:execute).
          with("SELECT ? FROM posts WHERE ? = ?", [:id], :published, false).
          and_return result_stub({:id => 1}, {:id => 2})

        connection.should_receive(:execute).
          with "UPDATE posts SET ? = ? WHERE ? IN (?)", :title, 'Cequel', :id, [1, 2]

        scope.update_all(:title => 'Cequel')
      end
    end

    context 'with scope selecting multiple values on non-key column' do
      let(:scope) { Post.where(:title => %w(Cequel Cassandra)) }

      it 'should perform multiple subqueries and execute single update on returned keys' do
        connection.stub(:execute).
          with("SELECT ? FROM posts WHERE ? = ?", [:id], :title, 'Cequel').
          and_return result_stub({:id => 1}, {:id => 2})

        connection.stub(:execute).
          with("SELECT ? FROM posts WHERE ? = ?", [:id], :title, 'Cassandra').
          and_return result_stub({:id => 3}, {:id => 4})

        connection.should_receive(:execute).
          with "UPDATE posts SET ? = ? WHERE ? IN (?)", :published, true, :id, [1, 2, 3, 4]

        scope.update_all(:published => true)
      end
    end
  end

  describe '#destroy_all' do
    context 'with no scope restrictions' do
      let(:scope) { Post }

      it 'should destroy all instances' do
        connection.should_receive(:execute).
          with('SELECT * FROM posts').
          and_return result_stub(
            {'id' => 1, 'title' => 'Cequel'},
            {'id' => 2, 'title' => 'Cassandra'}
          )
        connection.should_receive(:execute).
          with "DELETE FROM posts WHERE ? = ?", :id, 1
        connection.should_receive(:execute).
          with "DELETE FROM posts WHERE ? = ?", :id, 2
        scope.destroy_all
      end
    end

    context 'with column restrictions' do
      let(:scope) { Post.where(:id => [1, 2]) }

      it 'should issue scoped update request' do
        connection.should_receive(:execute).
          with("SELECT * FROM posts WHERE ? IN (?)", :id, [1, 2]).
          and_return result_stub(
            {'id' => 1, 'title' => 'Cequel'},
            {'id' => 2, 'title' => 'Cassandra'}
          )
        connection.should_receive(:execute).
          with "DELETE FROM posts WHERE ? = ?", :id, 1
        connection.should_receive(:execute).
          with "DELETE FROM posts WHERE ? = ?", :id, 2
        scope.destroy_all
      end

    end
  end

  describe '#delete_all' do
    context 'with no scope restrictions' do
      let(:scope) { Post }

      it 'should truncate keyspace' do
        connection.should_receive(:execute).
          with "TRUNCATE posts"
        Post.delete_all
      end
    end

    context 'with scope selecting on ids' do
      let(:scope) { Post.where(:id => [1, 2]) }

      it 'should issue scoped delete request' do
        connection.should_receive(:execute).
          with "DELETE FROM posts WHERE ? IN (?)", :id, [1, 2]
        scope.delete_all
      end

    end

    context 'with scope selecting on non-IDs' do
      let(:scope) { Post.where(:published => false) }

      it 'should perform "subquery" and issue update' do
        connection.stub(:execute).
          with("SELECT ? FROM posts WHERE ? = ?", [:id], :published, false).
          and_return result_stub({:id => 1}, {:id => 2})

        connection.should_receive(:execute).
          with "DELETE FROM posts WHERE ? IN (?)", :id, [1, 2]

        scope.delete_all
      end
    end

    context 'with scope selecting multiple values on non-key column' do
      let(:scope) { Post.where(:title => %w(Cequel Cassandra)) }

      it 'should perform multiple subqueries and execute single update on returned keys' do
        connection.stub(:execute).
          with("SELECT ? FROM posts WHERE ? = ?", [:id], :title, 'Cequel').
          and_return result_stub({:id => 1}, {:id => 2})

        connection.stub(:execute).
          with("SELECT ? FROM posts WHERE ? = ?", [:id], :title, 'Cassandra').
          and_return result_stub({:id => 3}, {:id => 4})

        connection.should_receive(:execute).
          with "DELETE FROM posts WHERE ? IN (?)", :id, [1, 2, 3, 4]

        scope.delete_all
      end
    end
  end

  describe '::default_scope' do
    it 'should include in scope by default' do
      connection.should_receive(:execute).
        with("SELECT * FROM blogs LIMIT 100").and_return result_stub

      Blog.all.to_a
    end

    it 'should override as with normal scope' do
      connection.should_receive(:execute).
        with("SELECT * FROM blogs LIMIT 1000").and_return result_stub

      Blog.limit(1000).to_a
    end
  end

end
