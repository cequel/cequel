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
  model :Post do
    key :blog_subdomain, :text
    key :id, :uuid, auto: true
    column :title, :text
    column :author, :text, serialize: :json
  end

  uuid :id

  let(:author) {
    Author.new(
      'Sue',
      'http://example.com/sue.jpg',
      'Sue has been writing about Cassandra since ...'
    )
  }

  describe 'using JSON' do
    let(:post) {
      Post.new(
        blog_subdomain: 'big-data',
        id: id,
        title: 'Cequel',
        author: author
      ).tap(&:save)
    }

    it 'serializes arbitrary objects' do
      post.raw_attributes[:author].should ==
        %{{"^o":"Author","name":"Sue","avatar":"http://example.com/sue.jpg","bio":"Sue has been writing about Cassandra since ..."}}
    end

    it 'should return the object instance' do
      post.author.should == author
    end

    it 'should return an identical object instance when reloading' do
      Post.at('big-data', post.id).first.author.tap do |a|
        a.name.should == author.name
        a.avatar.should == author.avatar
        a.bio.should == author.bio
      end
    end
  end

  it "does not allow unrecognized options" do
    expect {
      Post.class_eval do
        column :buffalo, :text, serialize: :xmlsonpack
      end
    }.to raise_error(ArgumentError)
  end
end
