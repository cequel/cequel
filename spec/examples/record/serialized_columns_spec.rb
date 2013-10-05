require_relative "spec_helper"

class Author
  attr_reader :name, :avatar, :bio

  def initialize(name, avatar, bio)
    @name = name
    @avatar = avatar
    @bio = bio
  end
end


describe 'serialized columns' do
  let(:author) {
    Author.new(
      'Sue',
      'http://example.com/sue.jpg',
      'Sue has been writing about Cassandra since ...'
    )
  }

  model :Post do
    key :blog_subdomain, :text
    key :id, :uuid, auto: true
    column :title, :text
    column :author, :text, serialize: :json
  end

  uuid :id

  it 'should serialize objects to json' do
    post = Post.new(
      blog_subdomain: 'big-data',
      id: id,
      title: 'Cequel',
      author: author
    ).tap(&:save)
    post.author.should == author
  end
end
