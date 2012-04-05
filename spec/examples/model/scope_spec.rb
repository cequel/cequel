require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Scope do
  describe '#each' do
    it 'should provide enumerator for query results' do
      connection.stub(:execute).with("SELECT * FROM posts").
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.all.map { |post| [post.id, post.title] }.
        should == [[1, 'Cequel']]
    end

    it 'should not enumerate results for missing keys' do
      connection.stub(:execute).with("SELECT * FROM posts"). #FIXME
        and_return result_stub(:id => 1)

      Post.all.to_a.should == []
    end

    it 'should provide enumerator if no block given' do
      enum = Post.all.each

      connection.stub(:execute).with("SELECT * FROM posts").
        and_return result_stub(:id => 1, :title => 'Cequel')

      enum.map { |post| post.title }.should == ['Cequel']
    end
  end

  describe '#first' do
    it 'should query for a single post' do
      connection.stub(:execute).with("SELECT * FROM posts LIMIT 1").
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.first.title.should == 'Cequel'
    end

    it 'should query for a single post within scope' do
      connection.stub(:execute).with("SELECT id, title FROM posts LIMIT 1").
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.select(:id, :title).first.title.should == 'Cequel'
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
        with("SELECT COUNT(*) FROM posts WHERE blog_id = 1").
        and_return result_stub('count' => 5)

      Post.where(:blog_id => 1).count.should == 5
    end
  end

  describe '#find' do
    it 'should send scoped query when no block is passed' do
      connection.stub(:execute).
        with("SELECT id, title FROM posts WHERE id = 1 LIMIT 1").
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.select(:id, :title).find(1).title.should == 'Cequel'
    end

    it 'should send scoped query with multiple keys when no block is passed' do
      connection.stub(:execute).
        with("SELECT id, title FROM posts WHERE id IN (1, 2)").
        and_return result_stub(
          {:id => 1, :title => 'Cequel'},
          {:id => 2, :title => 'Cequel 2'}
        )

      Post.select(:id, :title).find(1, 2).
        map { |post| post.title }.should == ['Cequel', 'Cequel 2']
    end

    it 'should delegate to enumerator when block is passed' do
      connection.stub(:execute).
        with("SELECT id, title FROM posts").
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
        with("SELECT COUNT(*) FROM posts WHERE blog_id = 1").
        and_return result_stub('count' => 5)

      Post.where(:blog_id => 1).any?.should be_true
    end

    it 'if called without block, should return false if COUNT == 0' do
      connection.stub(:execute).
        with("SELECT COUNT(*) FROM posts WHERE blog_id = 1").
        and_return result_stub('count' => 0)

      Post.where(:blog_id => 1).any?.should be_false
    end

    it 'if called with block should delegate to enumerator' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE blog_id = 1").
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
        with("SELECT COUNT(*) FROM posts WHERE blog_id = 1").
        and_return result_stub('count' => 5)

      Post.where(:blog_id => 1).none?.should be_false
    end

    it 'if called without block, should return true if COUNT == 0' do
      connection.stub(:execute).
        with("SELECT COUNT(*) FROM posts WHERE blog_id = 1").
        and_return result_stub('count' => 0)

      Post.where(:blog_id => 1).none?.should be_true
    end

    it 'if called with block should delegate to enumerator' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE blog_id = 1").
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
        with("SELECT COUNT(*) FROM posts WHERE blog_id = 1").
        and_return result_stub('count' => 0)

      Post.where(:blog_id => 1).one?.should be_false
    end

    it 'if called without block, should return true if COUNT == 1' do
      connection.stub(:execute).
        with("SELECT COUNT(*) FROM posts WHERE blog_id = 1").
        and_return result_stub('count' => 1)

      Post.where(:blog_id => 1).one?.should be_true
    end

    it 'if called without block, should return false if COUNT > 1' do
      connection.stub(:execute).
        with("SELECT COUNT(*) FROM posts WHERE blog_id = 1").
        and_return result_stub('count' => 12)

      Post.where(:blog_id => 1).one?.should be_false
    end

    it 'if called with block should delegate to enumerator' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE blog_id = 1").
        and_return result_stub(
          {:id => 1, :title => 'Cequel'},
          {:id => 2, :title => 'Cequel 2'}
        )

      Post.where(:blog_id => 1).none? { |post| post.id == 1 }.should be_false
      Post.where(:blog_id => 1).none? { |post| post.id == 8 }.should be_true
    end
  end

  describe '#select' do
    it 'should scope columns in data set' do
      connection.stub(:execute).with("SELECT id, title FROM posts").
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.select(:id, :title).map { |post| post.title }.should == ['Cequel']
    end

    it 'should delegate to enumerator if block given' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE blog_id = 1").
        and_return result_stub(
          {:id => 1, :title => 'Cequel'},
          {:id => 2, :title => 'Cequel 2'},
          {:id => 3, :title => 'Cequel 3'}
        )

      Post.where(:blog_id => 1).select { |post| post.id < 3 }.
        map { |post| post.title }.should == ['Cequel', 'Cequel 2']
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
      connection.stub(:execute).with("SELECT * FROM posts WHERE id = 1").
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.where(:id => 1).map { |post| post.title }.should == ['Cequel']
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
        with("SELECT id, title FROM posts USING CONSISTENCY QUORUM WHERE blog_id = 1 LIMIT 5").
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.select(:id, :title).
        consistency(:quorum).
        where(:blog_id => 1).
        limit(5).
        map { |post| post.title }.should == ['Cequel']
    end

    it 'should delegate unknown methods to the underlying class with self as current scope' do
      connection.stub(:execute).
        with("SELECT id, title FROM posts USING CONSISTENCY QUORUM WHERE blog_id = 1 LIMIT 5").
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.select(:id, :title).
        consistency(:quorum).
        for_blog(1).
        limit(5).
        map { |post| post.title }.should == ['Cequel']
    end

  end

end
