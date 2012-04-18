require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Associations do

  describe '::belongs_to' do
    let(:post) do
      Post.new(:id => 1).tap { |post| post.blog_id = 2 }
    end

    before do
      connection.stub(:execute).
        with('SELECT * FROM blogs WHERE id = 2 LIMIT 1').
        and_return result_stub(:id => 2, :name => 'Big Data Blog')
    end

    it 'should query column family and return associated model' do
      post.blog.name.should == 'Big Data Blog'
    end

    it 'should be nil if foreign key column is nil' do
      post.blog_id = nil
      post.blog.should be_nil
    end

    it 'should memoize instance' do
      post.blog
      connection.should_not_receive :execute
      post.blog
    end

    it 'should unmemoize instance if foreign key is changed' do
      post.blog
      post.blog_id = 3
      connection.stub(:execute).
        with('SELECT * FROM blogs WHERE id = 3 LIMIT 1').
        and_return result_stub(:id => 2, :name => 'Another Blog')

      post.blog.name.should == 'Another Blog'
    end

    it 'should provide setter for association' do
      post.blog = Blog.new(:id => 3, :name => 'This blog')
      post.blog_id.should == 3
    end

  end

  describe '::has_many' do
    let(:blog) do
      Blog.new(:id => 2)
    end

    before do
      connection.stub(:execute).
        with('SELECT * FROM posts WHERE blog_id = 2').
        and_return result_stub(
          {:id => 1, :title => 'Cequel'},
          {:id => 2, :title => 'Cequel revisited'}
        )
    end

    it 'should provide scope for associated instances' do
      blog.posts.map { |post| post.title }.should ==
        ['Cequel', 'Cequel revisited']
    end

    it 'should destroy associated instances if :dependent => :destroy' do
      connection.stub(:execute).with 'DELETE FROM blogs WHERE id = 2'
      connection.should_receive(:execute).with 'DELETE FROM posts WHERE id = 1'
      connection.should_receive(:execute).with 'DELETE FROM posts WHERE id = 2'
      blog.destroy
    end
  end

  describe '::has_one' do
    let(:post) { Post.new(:id => 1) }

    before do
      connection.stub(:execute).
        with("SELECT * FROM assets WHERE class_name = 'Photo' AND post_id = 1 LIMIT 1").
        and_return result_stub(
          {:id => 1, :type => 'Photo', :url => 'http://outofti.me/glamour.jpg'},
        )
    end

    it 'should look up association by foreign key' do
      post.thumbnail.url.should == 'http://outofti.me/glamour.jpg'
    end
  end
end
