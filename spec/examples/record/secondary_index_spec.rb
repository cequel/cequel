require_relative 'spec_helper'

describe Cequel::Record::SecondaryIndexes do
  model :Post do
    key :blog_subdomain, :text
    key :permalink, :text
    column :title, :text
    column :author_id, :uuid, :index => true
  end

  let(:uuids) { Array.new(2) { CassandraCQL::UUID.new }}

  let!(:posts) do
    3.times.map do |i|
      Post.create! do |post|
        post.blog_subdomain = 'bigdata'
        post.permalink = "cequel#{i}"
        post.title = "Cequel #{i}"
        post.author_id = uuids[i%2]
      end
    end
  end

  it 'should create secondary index in schema' do
    cequel.schema.read_table(:posts).data_columns.
      find { |column| column.name == :author_id }.index_name.
      should be
  end

  it 'should expose scope to query by secondary index' do
    Post.with_author_id(uuids.first).map(&:permalink).
      should == %w(cequel0 cequel2)
  end

  it 'should expose method to retrieve first result by secondary index' do
    Post.find_by_author_id(uuids.first).should == posts.first
  end

  it 'should expose method to eagerly retrieve all results by secondary index' do
    posts = Post.find_all_by_author_id(uuids.first)
    disallow_queries!
    posts.map(&:permalink).should == %w(cequel0 cequel2)
  end

end
