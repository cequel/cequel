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
      Post.new { |post| post.permalink = 'big-data' }.permalink.
        should == 'big-data'
    end

    it 'should cast key to correct value' do
      Post.new { |post| post.permalink = 44 }.permalink.
        should == '44'
    end

    it 'should have nil key if unset' do
      Post.new.permalink.should be_nil
    end

    it 'should provide accessor for data column' do
      Post.new { |post| post.title = 'Big Data' }.title.should == 'Big Data'
    end

    it 'should cast data column to correct value' do
      Post.new { |post| post.title = 'Big Data'.force_encoding('US-ASCII') }.
        title.encoding.name.should == 'UTF-8'
    end

    it 'should have nil data column value if unset' do
      Post.new.title.should be_nil
    end

    it 'should allow setting attributes via #attributes=' do
      Post.new.tap { |post| post.attributes = {:title => 'Big Data' }}.
        title.should == 'Big Data'
    end

    it 'should use writers when setting attributes' do
      Post.new.tap { |post| post.attributes = {:downcased_title => 'big data' }}.
        title.should == 'Big Data'
    end

    it 'should take attribute arguments to ::new' do
      Post.new(:downcased_title => 'big data').title.should == 'Big Data'
    end

    it 'should provide accessor for list column' do
      Post.new { |post| post.tags = %w(one two three) }.tags.
        should == %w(one two three)
    end

    it 'should cast collection in list column to list' do
      Post.new { |post| post.tags = Set['1', '2', '3'] }.tags.
        should == %w(1 2 3)
    end

    it 'should cast elements in list' do
      Post.new { |post| post.tags = [1, 2, 3] }.tags.should == %w(1 2 3)
    end

    it 'should have empty list column value if unset' do
      Post.new.tags.should == []
    end

    it 'should provide accessor for set column' do
      Post.new { |post| post.categories = Set['Big Data', 'Cassandra'] }.
        categories.should == Set['Big Data', 'Cassandra']
    end

    it 'should cast values in set column to correct type' do
      Post.new { |post| post.categories = Set[1, 2, 3] }.categories.
        should == Set['1', '2', '3']
    end

    it 'should cast collection to set in set column' do
      Post.new { |post| post.categories = ['1', '2', '3'] }.categories.
        should == Set['1', '2', '3']
    end

    it 'should have empty set column value if present' do
      Post.new.categories.should == Set[]
    end

    it 'should provide accessor for map column' do
      Post.new { |post| post.shares = {'facebook' => 1, 'twitter' => 2}}.
        shares.should == {'facebook' => 1, 'twitter' => 2}
    end

    it 'should cast values for map column' do
      Post.new { |post| post.shares = {facebook: '1', twitter: '2'} }.
        shares.should == {'facebook' => 1, 'twitter' => 2}
    end

    it 'should cast collection passed to map column to map' do
      Post.new { |post| post.shares = [['facebook', 1], ['twitter', 2]] }.
        shares.should == {'facebook' => 1, 'twitter' => 2}
    end

    it 'should set map column to empty hash by default' do
      Post.new.shares.should == {}
    end
  end

  describe 'configured property defaults' do
    model :Post do
      key :permalink, :text
      column :title, :text, :default => 'New Post'
      list :tags, :text, :default => ['new']
      set :categories, :text, :default => Set['Big Data']
      map :shares, :text, :int, :default => {'facebook' => 0}
    end

    it 'should respect default for data column' do
      Post.new.title.should == 'New Post'
    end

    it 'should respect default for list column' do
      Post.new.tags.should == ['new']
    end

    it 'should respect default for set column' do
      Post.new.categories.should == Set['Big Data']
    end

    it 'should respect default for map column' do
      Post.new.shares.should == {'facebook' => 0}
    end
  end

  describe 'dynamic property generation' do
    model :Post do
      key :id, :uuid, auto: true
      column :title, :text, default: -> { "Post #{Date.today}" }
    end

    it 'should auto-generate UUID key' do
      Post.new.id.should be_a(CassandraCQL::UUID)
    end

    it 'should raise ArgumentError if auto specified for non-UUID' do
      expect do
        Class.new do
          include Cequel::Record
          key :subdomain, :text, auto: true
        end
      end.to raise_error(ArgumentError)
    end

    it 'should run default proc' do
      Post.new.title.should == "Post #{Date.today}"
    end
  end
end
