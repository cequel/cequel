# -*- encoding : utf-8 -*-
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

    cattr_accessor :callback_count
    self.callback_count = 0
    before_destroy { self.class.callback_count += 1 }
  end

  model :Attachment do
    belongs_to :post
    key :id, :uuid, auto: true
    column :caption, :text

    cattr_accessor :callback_count
    self.callback_count = 0
    before_destroy { self.class.callback_count += 1 }
  end

  model :Photo do
    belongs_to :post, partition: true
    key :id, :uuid, auto: true
    column :caption, :text
  end

  describe '::belongs_to' do
    let(:blog) { Blog.new { |blog| blog.subdomain = 'big-data' }}
    let(:post) { Post.new }

    it 'should add parent key as first key' do
      expect(Post.key_column_names.first).to eq(:blog_subdomain)
    end

    it 'should add parent key as the partition key' do
      expect(Post.partition_key_column_names).to eq([:blog_subdomain])
    end

    it "should add parent's keys as first keys" do
      expect(Comment.key_column_names.first(2)).to eq([:post_blog_subdomain, :post_id])
    end

    it "should add parent's first key as partition key" do
      expect(Comment.partition_key_column_names).to eq([:post_blog_subdomain])
    end

    it 'should provide accessors for association object' do
      post.blog = blog
      expect(post.blog).to eq(blog)
    end

    it 'should set parent key(s) when setting association object' do
      post.blog = blog
      expect(post.blog_subdomain).to eq('big-data')
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
      expect(post.blog.subdomain).to eq('big-data')
    end

    it 'should not hydrate parent instance when creating from key' do
      post.blog_subdomain = 'big-data'
      disallow_queries!
      expect(post.blog).not_to be_loaded
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

    context "with partition => true" do
      let(:post) { Post.new { |post| post.blog_subdomain = 'big-data' }}
      let(:photo) { Photo.new }

      it "should add parent's keys as first keys" do
        expect(Photo.key_column_names.first(2)).to eq([:post_blog_subdomain, :post_id])
      end

      it "should add parent's keys as partition keys" do
        expect(Photo.partition_key_column_names).to eq([:post_blog_subdomain, :post_id])
      end

      it 'should provide accessors for association object' do
        photo.post = post
        expect(photo.post).to eq(post)
      end

      it 'should set parent key(s) when setting association object' do
        photo.post = post
        expect(photo.post_blog_subdomain).to eq('big-data')
        expect(photo.post_id).to eq(post.id)
      end

      it 'should raise ArgumentError when parent is set without a key' do
        post.blog_subdomain = nil
        expect { photo.post = post }.to raise_error(ArgumentError)
      end

      it 'should raise ArgumentError when parent is set to wrong class' do
        expect { photo.post = photo }.to raise_error(ArgumentError)
      end

      it 'should return Photo instance when parent keys are set directly' do
        photo.post_blog_subdomain = 'big-data'
        photo.post_id = post.id
        expect(photo.post).to eq(post)
      end

      it 'should not hydrate parent instance when creating from keys' do
        photo.post_blog_subdomain = 'big-data'
        photo.post_id = post.id
        disallow_queries!
        expect(photo.post).not_to be_loaded
      end

      it 'should not allow declaring belongs_to after key' do
        expect do
          Class.new do
            include Cequel::Record
            key :permalink, :text
            belongs_to :post, partition: true
          end
        end.to raise_error(Cequel::Record::InvalidRecordConfiguration)
      end

      it 'should not allow declaring belongs_to more than once' do
        expect do
          Class.new do
            include Cequel::Record
            belongs_to :post, partition: true
            belongs_to :user
          end
        end.to raise_error(Cequel::Record::InvalidRecordConfiguration)
      end
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
      expect(blog.posts.map(&:title)).to eq(["Post 0", "Post 1", "Post 2"])
    end

    it 'should retain scope when hydrated multiple times' do
      blog.posts.map(&:id)
      disallow_queries!
      expect(blog.posts.map(&:title)).to eq(["Post 0", "Post 1", "Post 2"])
    end

    it 'should reload when reload argument passed' do
      blog.posts.map(&:id)
      posts.first.destroy
      expect(blog.posts(true).map(&:title)).to eq(['Post 1', 'Post 2'])
    end

    it 'should support #find with key' do
      expect(blog.posts.find(posts.first.id)).to eq(posts.first)
    end

    it 'should support #find with block' do
      expect(blog.posts.find { |post| post.title.include?('1') }).to eq(posts[1])
    end

    it 'should support #select with block' do
      expect(blog.posts.select { |post| !post.title.include?('2') })
        .to eq(posts.first(2))
    end

    it 'should support #select with arguments' do
      expect { blog.posts.select(:title).first.id }
        .to raise_error(Cequel::Record::MissingAttributeError)
    end

    it 'should load #first directly from the database if unloaded' do
      blog.posts.first.title
      expect(blog.posts).not_to be_loaded
    end

    it 'should read #first from loaded collection' do
      blog.posts.entries
      disallow_queries!
      expect(blog.posts.first.title).to eq('Post 0')
    end

    it 'should always query the database for #count' do
      blog.posts.entries
      posts.first.destroy
      expect(blog.posts.count).to eq(2)
    end

    it 'should always load the records for #length' do
      expect(blog.posts.length).to eq(3)
      expect(blog.posts).to be_loaded
    end

    it 'should count from database for #size if unloaded' do
      expect(blog.posts.size).to eq(3)
      expect(blog.posts).not_to be_loaded
    end

    it 'should count records in memory for #size if loaded' do
      blog.posts.entries
      disallow_queries!
      expect(blog.posts.size).to eq(3)
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
      end

      it "deletes all children when destroying the parent" do
        expect {
          post_with_comments.destroy
        }.to change { Comment.count }.by(-2)
      end

      it "executes :destroy callbacks on the children" do
        expect {
          post_with_comments.destroy
        }.to change { Comment.callback_count }.by(2)
      end
    end

    context "with :dependent => :delete" do
      let(:post_with_attachments) { posts.first }

      before :each do
        2.times.map do |i|
          Attachment.new do |comment|
            comment.caption = "cat #{i} is awesome"
            comment.post = post_with_attachments
          end.tap(&:save)
        end
      end

      it "deletes all children when destroying the parent" do
        expect {
          post_with_attachments.destroy
        }.to change { Attachment.count }.by(-2)
      end

      it "does not execute :destroy callbacks on the children" do
        expect {
          post_with_attachments.destroy
        }.not_to change { Attachment.callback_count }
      end
    end
  end
end
