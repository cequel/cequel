# -*- encoding : utf-8 -*-
require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record::Properties do

  describe 'property accessors' do
    model :Post do
      key :permalink, :text
      column :title, :text
      list :tags, :text
      set :categories, :text
      map :shares, :text, :int

      def downcased_title=(downcased_title)
        self.title = downcased_title.titleize
      end
    end

    it 'should provide accessor for key' do
      expect(Post.new { |post| post.permalink = 'big-data' }.permalink).
        to eq('big-data')
    end

    it 'should cast key to correct value' do
      expect(Post.new { |post| post.permalink = 44 }.permalink).
        to eq('44')
    end

    it 'should have nil key if unset' do
      expect(Post.new.permalink).to be_nil
    end

    it 'should provide accessor for data column' do
      expect(Post.new { |post| post.title = 'Big Data' }.title).to eq('Big Data')
    end

    it 'should cast data column to correct value' do
      expect(Post.new { |post| post.title = 'Big Data'.force_encoding('US-ASCII') }.
        title.encoding.name).to eq('UTF-8')
    end

    it 'should have nil data column value if unset' do
      expect(Post.new.title).to be_nil
    end

    it 'should allow setting attributes via #attributes=' do
      expect(Post.new.tap { |post| post.attributes = {:title => 'Big Data' }}.
        title).to eq('Big Data')
    end

    it 'should use writers when setting attributes' do
      expect(Post.new.tap { |post| post.attributes = {:downcased_title => 'big data' }}.
        title).to eq('Big Data')
    end

    it 'should get attributes with indifferent access' do
      post = Post.new.tap { |post| post.attributes = {:downcased_title => 'big data' }}
      expect(post.attributes[:title]).to eq 'Big Data'
      expect(post.attributes["title"]).to eq 'Big Data'
    end

    it 'should take attribute arguments to ::new' do
      expect(Post.new(:downcased_title => 'big data').title).to eq('Big Data')
    end

    it 'should provide accessor for list column' do
      expect(Post.new { |post| post.tags = %w(one two three) }.tags).to eq(
        %w(one two three))
    end

    it 'should cast collection in list column to list' do
      expect(Post.new { |post| post.tags = Set['1', '2', '3'] }.tags)
        .to eq(%w(1 2 3))
    end

    it 'should cast elements in list' do
      expect(Post.new { |post| post.tags = [1, 2, 3] }.tags).to eq(%w(1 2 3))
    end

    it 'should have empty list column value if unset' do
      expect(Post.new.tags).to eq([])
    end

    it 'should have empty list column value if unset in database' do
      uniq_key = SecureRandom.uuid
      Post.create! permalink: uniq_key
      expect(Post[uniq_key].tags).to eq([])
    end


    it 'should provide accessor for set column' do
      expect(Post.new { |post| post.categories = Set['Big Data', 'Cassandra'] }
        .categories).to eq(Set['Big Data', 'Cassandra'])
    end

    it 'should cast values in set column to correct type' do
      expect(Post.new { |post| post.categories = Set[1, 2, 3] }.categories)
        .to eq(Set['1', '2', '3'])
    end

    it 'should cast collection to set in set column' do
      expect(Post.new { |post| post.categories = ['1', '2', '3'] }.categories)
        .to eq(Set['1', '2', '3'])
    end

    it 'should have empty set column value if not explicitly set' do
      expect(Post.new.categories).to eq(Set[])
    end

    it 'should handle saved records with unspecified set properties' do
      uuid = SecureRandom.uuid
      Post.create!(permalink: uuid)
      expect(Post[uuid].categories).to eq(::Set[])
    end

    it 'should provide accessor for map column' do
      expect(Post.new { |post| post.shares = {'facebook' => 1, 'twitter' => 2}}
        .shares).to eq({'facebook' => 1, 'twitter' => 2})
    end

    it 'should cast values for map column' do
      expect(Post.new { |post| post.shares = {facebook: '1', twitter: '2'} }
        .shares).to eq({'facebook' => 1, 'twitter' => 2})
    end

    it 'should cast collection passed to map column to map' do
      expect(Post.new { |post| post.shares = [['facebook', 1], ['twitter', 2]] }
        .shares).to eq({'facebook' => 1, 'twitter' => 2})
    end

    it 'should set map column to empty hash by default' do
      expect(Post.new.shares).to eq({})
    end

    it 'should handle saved records with unspecified map properties' do
      uuid = SecureRandom.uuid
      Post.create!(permalink: uuid)
      expect(Post[uuid].shares).to eq({})
    end


  end

  describe 'configured property defaults' do
    model :Post do
      key :permalink, :text, :default => 'new_permalink'
      column :title, :text, :default => 'New Post'
      list :tags, :text, :default => ['new']
      set :categories, :text, :default => Set['Big Data']
      map :shares, :text, :int, :default => {'facebook' => 0}
    end

    it 'should respect default for keys' do
      expect(Post.new.permalink).to eq('new_permalink')
    end

    it 'should respect default for data column' do
      expect(Post.new.title).to eq('New Post')
    end

    it 'should respect default for list column' do
      expect(Post.new.tags).to eq(['new'])
    end

    it 'should respect default for set column' do
      expect(Post.new.categories).to eq(Set['Big Data'])
    end

    it 'should respect default for map column' do
      expect(Post.new.shares).to eq({'facebook' => 0})
    end
  end

  describe 'dynamic property generation' do
    model :Post do
      key :id, :uuid, auto: true
      key :subid, :text, default: -> { "subid #{1+1}" }
      column :title, :text, default: -> { "Post #{Date.today}" }
    end

    it 'should auto-generate UUID key' do
      expect(Cequel.uuid?(Post.new.id)).to eq(true)
    end

    it 'should raise ArgumentError if auto specified for non-UUID' do
      expect do
        Class.new do
          include Cequel::Record
          key :subdomain, :text, auto: true
        end
      end.to raise_error(ArgumentError)
    end

    it 'should run default proc on keys' do
      expect(Post.new.subid).to eq("subid #{1+1}") 
    end

    it 'should run default proc' do
      expect(Post.new.title).to eq("Post #{Date.today}")
    end
  end
end
