require_relative 'spec_helper'

describe Cequel::Record::Associations do

  model :Blog do
    key :subdomain, :text
    column :name, :text

    has_many :posts
  end

  model :User do
    key :login, :text
    column :name, :text
  end

  model :Post do
    belongs_to :blog
    key :id, :uuid, auto: true
    column :title, :text

    has_many :comments, dependent: :destroy
    has_many :attachments, dependent: :delete
  end

  model :Comment do
    belongs_to :post
    key :id, :uuid, auto: true
    column :content, :text
  end

  model :Attachment do
    belongs_to :post
    key :id, :uuid, auto: true
    column :caption, :text
  end

  describe '::belongs_to' do
    let(:blog) { Blog.new { |blog| blog.subdomain = 'big-data' }}
    let(:post) { Post.new }

    it 'should add parent key as first key' do
      Post.key_column_names.first.should == :blog_subdomain
    end

    it 'should provide accessors for association object' do
      post.blog = blog
      post.blog.should == blog
    end

    it 'should set parent key(s) when setting association object' do
      post.blog = blog
      post.blog_subdomain.should == 'big-data'
    end

    it 'should raise ArgumentError when parent is set without keys' do
      blog.subdomain = nil
      expect { post.blog = blog }.to raise_error(ArgumentError)
    end

    it 'should raise ArgumentError when parent is set to wrong class' do
      expect { post.blog = post }.to raise_error(ArgumentError)
    end

    it 'should return Blog instance when parent key set directly' do
      post.blog_subdomain = 'big-data'
      post.blog.subdomain.should == 'big-data'
    end

    it 'should not hydrate parent instance when creating from key' do
      post.blog_subdomain = 'big-data'
      disallow_queries!
      post.blog.should_not be_loaded
    end

    it 'should not allow declaring belongs_to after key' do
      expect do
        Class.new do
          include Cequel::Record
          key :permalink, :text
          belongs_to :blog
        end
      end.to raise_error(Cequel::Record::InvalidRecordConfiguration)
    end

    it 'should not allow declaring belongs_to more than once' do
      expect do
        Class.new do
          include Cequel::Record
          belongs_to :blog
          belongs_to :user
        end
      end.to raise_error(Cequel::Record::InvalidRecordConfiguration)
    end

  end

  describe '::has_many' do
    let(:blog) { Blog.new { |blog| blog.subdomain = 'cequel' }.tap(&:save) }
    let!(:posts) do
      3.times.map do |i|
        Post.new do |post|
          post.blog = blog
          post.title = "Post #{i}"
        end.tap(&:save)
      end
    end
    let!(:other_posts) do
      3.times.map do |i|
        Post.new do |post|
          post.blog_subdomain = 'mycat'
          post.title = "My Cat #{i}"
        end.tap(&:save)
      end
    end

    it 'should return scope of posts' do
      blog.posts.map(&:title).should == ["Post 0", "Post 1", "Post 2"]
    end

    it 'should retain scope when hydrated multiple times' do
      blog.posts.map(&:id)
      disallow_queries!
      blog.posts.map(&:title).should == ["Post 0", "Post 1", "Post 2"]
    end

    it 'should reload when reload argument passed' do
      blog.posts.map(&:id)
      posts.first.destroy
      blog.posts(true).map(&:title).should == ['Post 1', 'Post 2']
    end

    it "does not allow invalid :dependent options" do
      expect {
        Post.class_eval do
          has_many :users, dependent: :bar
        end
      }.to raise_error(ArgumentError)
    end

    it "does not allow unrecognized options" do
      expect {
        Post.class_eval do
          has_many :users, bogus: :buffalo
        end
      }.to raise_error(ArgumentError)
    end

    context "with dependent => destroy" do
      let(:post_with_comments) { posts.first }

      before :each do
        2.times.map do |i|
          Comment.new do |comment|
            comment.content = "cat #{i} is awesome"
            comment.post = post_with_comments
          end.tap(&:save)
        end
        @callback_count = 0
        Comment.any_instance.stub(:run_callbacks).with(:destroy) do
          @callback_count += 1
        end
      end

      it "deletes all children when destroying the parent" do
        expect {
          post_with_comments.destroy
        }.to change { Comment.count }.by(-2)
      end

      it "executes :destroy callbacks on the children" do
        expect {
          post_with_comments.destroy
        }.to change { @callback_count }.by(2)
      end
    end

    context "with dependent => delete" do
      let(:post_with_attachments) { posts.first }

      before :each do
        2.times.map do |i|
          Attachment.new do |comment|
            comment.caption = "cat #{i} is awesome"
            comment.post = post_with_attachments
          end.tap(&:save)
        end
        @callback_count = 0
        Attachment.any_instance.stub(:run_callbacks).with(:destroy) do
          @callback_count += 1
        end
      end

      it "deletes all children when destroying the parent" do
        expect {
          post_with_attachments.destroy
        }.to change { Attachment.count }.by(-2)
      end

      it "executes :destroy callbacks on the children" do
        expect {
          post_with_attachments.destroy
        }.to change { @callback_count }.by(0)
      end
    end
  end
end
