require_relative 'spec_helper'

describe Cequel::Model::Associations do

  model :Blog do
    key :subdomain, :text
    column :name, :text
  end

  model :User do
    key :login, :text
    column :name, :text
  end

  model :Post do
    belongs_to :blog
    key :id, :uuid
    column :title, :text
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
        Class.new(Cequel::Model::Base) do
          key :permalink, :text
          belongs_to :blog
        end
      end.to raise_error(Cequel::Model::InvalidRecordConfiguration)
    end

    it 'should not allow declaring belongs_to more than once' do
      expect do
        Class.new(Cequel::Model::Base) do
          belongs_to :blog
          belongs_to :user
        end
      end.to raise_error(Cequel::Model::InvalidRecordConfiguration)
    end

  end

end
