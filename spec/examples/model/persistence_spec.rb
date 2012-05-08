require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Persistence do
  describe '#find' do
    it 'should return hydrated instance' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE id = 2 LIMIT 1").
        and_return result_stub(:id => 2, :title => 'Cequel')

      post = Post.find(2)
      post.id.should == 2
      post.title.should == 'Cequel'
    end

    it 'should not set defaults when hydrating instance' do
      connection.stub(:execute).
        with("SELECT id, name FROM blogs WHERE id = 2 LIMIT 1").
        and_return result_stub(:id => 1, :title => 'Big Data')

      blog = Blog.select(:id, :name).find(2)
      blog.published.should be_nil
    end

    it 'should return multiple instances' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE id IN (2, 5)").
        and_return result_stub(
          {:id => 2, :title => 'Cequel 2'},
          {:id => 5, :title => 'Cequel 5'}
        )

      posts = Post.find(2, 5)
      posts.map { |post| [post.id, post.title] }.
        should == [[2, 'Cequel 2'], [5, 'Cequel 5']]
    end

    it 'should return one-element array if passed one-element array' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE id = 2 LIMIT 1").
        and_return result_stub(:id => 2, :title => 'Cequel')

      post = Post.find([2]).first
      post.id.should == 2
      post.title.should == 'Cequel'
    end

    it 'should raise RecordNotFound if row has no data' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE id = 2 LIMIT 1").
        and_return result_stub(:id => 2)

      expect { Post.find(2) }.to raise_error Cequel::Model::RecordNotFound
    end

    it 'should raise RecordNotFound if row has nil data' do
      connection.stub(:execute).
        with("SELECT title FROM posts WHERE id = 2 LIMIT 1").
        and_return result_stub(:title => nil)

      expect { Post.select(:title).find(2) }.to raise_error Cequel::Model::RecordNotFound
    end

    it 'should raise RecordNotFound if some rows in multi-row query have no data' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE id IN (2, 5)").
        and_return result_stub(
          {:id => 2, :title => 'Cequel 2'},
          {:id => 5}
        )

      expect { Post.find(2, 5) }.to raise_error(Cequel::Model::RecordNotFound)
    end
  end

  describe '#reload' do
    let(:post) do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE id = 2 LIMIT 1").
        and_return result_stub(:id => 2, :title => 'Cequel')
      Post.find(2)
    end

    it 'should reload attributes from Cassandra' do
      post.title = 'Donkeys'
      connection.should_receive(:execute).
        with("SELECT * FROM posts WHERE id = 2 LIMIT 1").
        and_return result_stub(:id => 2, :title => 'Cequel')
      post.reload
      post.title.should == 'Cequel'
    end
  end

  describe '#save' do
    describe 'with new record' do
      let(:post) { Post.new(:id => 1) }

      it 'should persist only columns with values' do
        connection.should_receive(:execute).
          with("INSERT INTO posts (id, title) VALUES (1, 'Cequel')")

        post.title = 'Cequel'
        post.save
      end

      it 'should mark instance as persisted' do
        connection.stub(:execute).
          with("INSERT INTO posts (id, title) VALUES (1, 'Cequel')")

        post.title = 'Cequel'
        post.save
        post.should be_persisted
      end

      it 'should not send anything to Cassandra if no column values are set' do
        post.save
        post.should_not be_persisted
      end

      it 'should raise MissingKey if no key set' do
        expect { Post.new.save }.to raise_error(Cequel::Model::MissingKey)
      end
    end

    describe 'with persisted record' do
      let(:post) do
        connection.stub(:execute).with("SELECT * FROM posts WHERE id = 1 LIMIT 1").
          and_return result_stub(:id => 1, :blog_id => 1, :title => 'Cequel')
        Post.find(1)
      end

      it 'should send UPDATE statement with changed columns' do
        connection.should_receive(:execute).
          with "UPDATE posts SET body = 'Cequel cequel' WHERE id = 1"
        post.body = 'Cequel cequel'
        post.save
      end

      it 'should send DELETE statement with removed columns' do
        connection.should_receive(:execute).
          with "DELETE title FROM posts WHERE id = 1"
        post.title = nil
        post.save
      end

      it 'should mark record as transient if all attributes removed' do
        connection.stub(:execute).
          with "DELETE title, blog_id FROM posts WHERE id = 1"
        post.title = nil
        post.blog_id = nil
        post.save
        post.should_not be_persisted
      end
    end
  end

  describe '#update_attributes' do
    let(:post) do
      connection.stub(:execute).with("SELECT * FROM posts WHERE id = 1 LIMIT 1").
        and_return result_stub(:id => 1, :blog_id => 1, :title => 'Cequel')
      Post.find(1)
    end

    it 'should change attributes and save them' do
      connection.should_receive(:execute).
        with "UPDATE posts SET body = 'Cequel cequel' WHERE id = 1"
      post.update_attributes(:body => 'Cequel cequel')
    end
  end

  describe '#destroy' do
    let(:post) do
      connection.stub(:execute).with("SELECT * FROM posts WHERE id = 1 LIMIT 1").
        and_return result_stub(:id => 1, :blog_id => 1, :title => 'Cequel')
      Post.find(1)
    end

    it 'should delete all columns from column family' do
      connection.should_receive(:execute).
        with "DELETE FROM posts WHERE id = 1"

      post.destroy
    end
  end

  describe '::create' do
    it 'should persist only columns with values' do
      connection.should_receive(:execute).
        with("INSERT INTO posts (id, title) VALUES (1, 'Cequel')")

      Post.create(:id => 1, :title => 'Cequel')
    end

    it 'should return post instance and mark it as persisted' do
      connection.stub(:execute).
        with("INSERT INTO posts (id, title) VALUES (1, 'Cequel')")

      Post.create(:id => 1, :title => 'Cequel').should be_persisted
    end
  end
end
