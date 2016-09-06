require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record::Timestamps do
  model :Blog do
    key :subdomain, :text
    column :name, :text
    timestamps
  end

  model :Post do
    key :blog_subdomain, :text
    key :id, :timeuuid, auto: true
    column :name, :text
    timestamps
  end

  let!(:now) { Timecop.freeze }

  context 'with simple primary key' do
    let!(:blog) { Blog.create!(subdomain: 'bigdata') }

    it 'should populate created_at after create new record' do
      expect(blog.created_at).to be_within(one_millisecond).of(now)
    end

    it 'should populate updated_at after create new record' do
      expect(blog.updated_at).to be_within(one_millisecond).of(now)
    end

    it 'should update updated_at after record update but not created_at' do
      future = Timecop.freeze(now + 2.minutes)
      blog.name = 'name'
      blog.save!
      expect(blog.updated_at).to be_within(one_millisecond).of(future)
    end

    it 'should cast the timestamp in the same way that Cassandra records it' do
      expect(Blog.first.updated_at).to eq(blog.updated_at)
    end
  end

  context 'with auto-generated timeuuid primary key' do
    let!(:post) { Post['bigdata'].create! }

    it 'should not have created_at column' do
      expect(Post.column_names).not_to include(:created_at)
    end

    it 'should expose created_at' do
      expect(post.created_at).to be_within(one_millisecond).of(now)
    end

    it 'should populate updated_at after create new record' do
      expect(post.updated_at).to be_within(one_millisecond).of(now)
    end

    it 'should update updated_at after record update but not created_at' do
      future = Timecop.freeze(now + 2.minutes)
      post.name = 'name'
      post.save!
      expect(post.created_at).to be_within(one_millisecond).of(now)
      expect(post.updated_at).to be_within(one_millisecond).of(future)
    end
  end
end
