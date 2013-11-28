require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record::RecordSet do
  model :Blog do
    key :subdomain, :ascii
    column :name, :text
    column :description, :text
  end

  model :Post do
    key :blog_subdomain, :ascii
    key :permalink, :ascii
    column :title, :text
    column :body, :text
    column :author_id, :uuid, index: true
    column :author_name, :text, index: true
    list :tags, :text
    set :categories, :text
    map :shares, :text, :int

    def self.latest(count)
      reverse.limit(count)
    end
  end

  model :Comment do
    key :blog_subdomain, :text
    key :permalink, :text
    key :id, :uuid, :auto => true
    column :body, :text
  end

  model :PublishedPost do
    key :blog_subdomain, :ascii
    key :published_at, :timeuuid
    column :permalink, :ascii, index: true
  end

  let(:subdomains) { blogs.map(&:subdomain) }
  let(:uuids) { Array.new(2) { CassandraCQL::UUID.new }}
  let(:now) { Time.at(Time.now.to_i) }

  let(:blogs) do
    3.times.map do |i|
      Blog.new do |blog|
        blog.subdomain = "blog-#{i}"
        blog.name = "Blog #{i}"
        blog.description = "This is Blog number #{i}"
      end
    end
  end

  let(:cassandra_posts) do
    5.times.map do |i|
      Post.new(
        :blog_subdomain => 'cassandra',
        :permalink => "cequel#{i}",
        :title => "Cequel #{i}",
        :body => "Post number #{i}",
        :author_id => uuids[i%2]
      )
    end
  end

  let(:published_posts) do
    5.times.map do |i|
      PublishedPost.new(
        :blog_subdomain => 'cassandra',
        :published_at => max_uuid(now + (i - 4).minutes),
        :permalink => "cequel#{i}"
      )
    end
  end

  let(:postgres_posts) do
    5.times.map do |i|
      Post.new(
        :blog_subdomain => 'postgres',
        :permalink => "sequel#{i}",
        :title => "Sequel #{i}"
      )
    end
  end

  let(:mongo_posts) do
    5.times.map do |i|
      Post.new(
        :blog_subdomain => 'mongo',
        :permalink => "mongoid#{i}",
        :title => "Mongoid #{i}"
      )
    end
  end

  let(:orm_posts) do
    5.times.map do |i|
      Post.new(
        :blog_subdomain => 'orms',
        :permalink => "cequel#{i}",
        :title => "Cequel ORM #{i}"
      )
    end
  end

  let(:posts) { [*cassandra_posts, *postgres_posts] }

  let(:comments) do
    5.times.map do |i|
      Comment.new(
        :blog_subdomain => 'cassandra',
        :permalink => 'cequel0',
        :id => CassandraCQL::UUID.new(Time.now - 5 + i),
        :body => "Comment #{i}"
      )
    end
  end

  let(:records) { posts }

  before { cequel.batch { records.flatten.each { |record| record.save! }}}

  describe '::find' do
    context 'simple primary key' do
      let(:records) { blogs }
      subject { Blog.find('blog-0') }

      its(:subdomain) { should == 'blog-0' }
      its(:name) { should == 'Blog 0' }

      it { should be_persisted }
      it { should_not be_transient }
      specify { Blog.new.should_not be_persisted }
      specify { Blog.new.should be_transient }

      it 'should cast argument to correct type' do
        Blog.find('blog-0'.force_encoding('ASCII-8BIT')).should be
      end

      it 'should raise RecordNotFound if bad argument passed' do
        expect { Blog.find('bogus') }.
          to raise_error(Cequel::Record::RecordNotFound)
      end
    end

    context 'compound primary key' do
      let(:records) { cassandra_posts }
      subject { Post['cassandra'].find('cequel0') }

      its(:blog_subdomain) { should == 'cassandra' }
      its(:permalink) { should == 'cequel0' }
      its(:title) { should == 'Cequel 0' }

      it { should be_persisted }
      it { should_not be_transient }
      specify { Post.new.should_not be_persisted }
      specify { Post.new.should be_transient }

      it 'should cast all keys to correct type' do
        Post['cassandra'.force_encoding('ASCII-8BIT')].
          find('cequel0'.force_encoding('ASCII-8BIT')).should be
      end

      it 'should raise RecordNotFound if bad argument passed' do
        expect { Post['cequel'].find('bogus')}.
          to raise_error(Cequel::Record::RecordNotFound)
      end
    end
  end

  describe '::[]' do
    context 'fully specified simple primary key' do
      let(:records) { blogs }
      subject { Blog['blog-0'] }

      it 'should not query the database' do
        disallow_queries!
        subject.subdomain.should == 'blog-0'
      end

      it 'should lazily query the database when attribute accessed' do
        subject.name.should == 'Blog 0'
      end

      it 'should get all eager-loadable attributes on first lazy load' do
        subject.name
        disallow_queries!
        subject.description.should == 'This is Blog number 0'
      end

      it 'should cast argument' do
        subject.subdomain.encoding.name.should == 'US-ASCII'
      end
    end

    context 'multiple simple primary keys' do
      let(:records) { blogs }
      subject { Blog['blog-0', 'blog-1'] }

      it 'should return both specified records' do
        subject.map(&:subdomain).should =~ %w(blog-0 blog-1)
      end

      it 'should not query the database' do
        disallow_queries!
        subject.map(&:subdomain)
      end

      it 'should load value lazily' do
        subject.first.name.should == 'Blog 0'
      end

      it 'should load values for all referenced records on first access' do
        max_statements! 1
        subject.first.name.should == 'Blog 0'
        subject.last.name.should == 'Blog 1'
      end
    end

    context 'fully specified compound primary key' do
      let(:records) { posts }
      subject { Post['cassandra']['cequel0'] }

      it 'should not query the database' do
        expect(cequel).not_to receive(:execute)
        subject.blog_subdomain.should == 'cassandra'
        subject.permalink.should == 'cequel0'
      end

      it 'should cast all keys to the correct type' do
        subject.blog_subdomain.encoding.name.should == 'US-ASCII'
        subject.permalink.encoding.name.should == 'US-ASCII'
      end

      it 'should lazily query the database when attribute accessed' do
        subject.title.should == 'Cequel 0'
      end

      it 'should get all eager-loadable attributes on first lazy load' do
        subject.title
        expect(cequel).not_to receive(:execute)
        subject.body.should == 'Post number 0'
      end
    end

    context 'fully specified compound primary key with multiple clustering columns' do
      let(:records) { posts }
      subject { Post['cassandra']['cequel0', 'cequel1'] }

      it 'should combine partition key with each clustering column' do
        disallow_queries!
        subject.map(&:key_values).
          should == [['cassandra', 'cequel0'], ['cassandra', 'cequel1']]
      end

      it 'should lazily load all records when one record accessed' do
        max_statements! 1
        subject.first.title.should == 'Cequel 0'
        subject.second.title.should == 'Cequel 1'
      end

      it 'should not allow collection columns to be selected' do
        expect { Post.select(:tags)['cassandra']['cequel0', 'cequel1'] }.
          to raise_error(ArgumentError)
      end
    end

    context 'partially specified compound primary key' do
      let(:records) { posts }
      it 'should create partial collection if not all keys specified' do
        Post['cassandra'].find_each(:batch_size => 2).map(&:title).
          should == (0...5).map { |i| "Cequel #{i}" }
      end
    end

    context 'partially specified compound primary key with multiple partition keys' do
      let(:records) { posts }
      subject { Post['cassandra', 'postgres'] }

      it 'should return scope to keys' do
        subject.map { |post| post.title }.should =~ (0...5).
          map { |i| ["Cequel #{i}", "Sequel #{i}"] }.flatten
      end
    end

    context 'fully specified compound primary key with multiple partition keys' do
      let(:records) { [posts, orm_posts] }

      subject { Post['cassandra', 'orms']['cequel0'] }

      it 'should return collection of unloaded models' do
        disallow_queries!
        subject.map(&:key_values).
          should == [['cassandra', 'cequel0'], ['orms', 'cequel0']]
      end

      it 'should lazy-load all records when properties of one accessed' do
        max_statements! 1
        subject.first.title.should == 'Cequel 0'
        subject.second.title.should == 'Cequel ORM 0'
      end
    end
  end

  describe '#all' do
    let(:records) { blogs }

    it 'should return all the records' do
      Blog.all.map(&:subdomain).should =~ subdomains
    end
  end

  describe '#find_each' do
    let(:records) { [posts, blogs, mongo_posts] }

    it 'should respect :batch_size argument' do
      cequel.should_receive(:execute).twice.and_call_original
      Blog.find_each(:batch_size => 2).map(&:subdomain).
        should =~ subdomains
    end
    it 'should iterate over all keys' do
      Post.find_each(:batch_size => 2).map(&:title).should =~
        (0...5).flat_map { |i| ["Cequel #{i}", "Sequel #{i}", "Mongoid #{i}"] }
    end
  end

  describe '#[]' do
    it 'should return partial collection' do
      Post['cassandra'].find_each(:batch_size => 2).map(&:title).
        should == (0...5).map { |i| "Cequel #{i}" }
    end

    it 'should cast arguments correctly' do
      Post['cassandra'.force_encoding('ASCII-8BIT')].
        find_each(:batch_size => 2).map(&:title).
        should == (0...5).map { |i| "Cequel #{i}" }
    end
  end

  describe '#/' do
    it 'should behave like #[]' do
      (Post / 'cassandra').find_each(:batch_size => 2).map(&:title).
        should == (0...5).map { |i| "Cequel #{i}" }
    end
  end

  describe '#after' do
    let(:records) { [posts, published_posts] }

    it 'should return collection after given key' do
      Post['cassandra'].after('cequel1').map(&:title).
        should == (2...5).map { |i| "Cequel #{i}" }
    end

    it 'should cast argument' do
      Post['cassandra'].after('cequel1'.force_encoding('ASCII-8BIT')).
        map(&:title).should == (2...5).map { |i| "Cequel #{i}" }
    end

    it 'should query Time range for Timeuuid key' do
      PublishedPost['cassandra'].after(now - 3.minutes).map(&:permalink).
        should == %w(cequel2 cequel3 cequel4)
    end
  end

  describe '#from' do
    let(:records) { [posts, published_posts] }

    it 'should return collection starting with given key' do
      Post['cassandra'].from('cequel1').map(&:title).
        should == (1...5).map { |i| "Cequel #{i}" }
    end

    it 'should cast argument' do
      Post['cassandra'].from('cequel1'.force_encoding('ASCII-8BIT')).
        map(&:title).should == (1...5).map { |i| "Cequel #{i}" }
    end

    it 'should query Time range for Timeuuid key' do
      PublishedPost['cassandra'].from(now - 3.minutes).map(&:permalink).
        should == %w(cequel1 cequel2 cequel3 cequel4)
    end

    it 'should raise ArgumentError when called on partition key' do
      expect { Post.from('cassandra') }.
        to raise_error(Cequel::Record::IllegalQuery)
    end
  end

  describe '#before' do
    let(:records) { [posts, published_posts] }

    it 'should return collection before given key' do
      Post['cassandra'].before('cequel3').map(&:title).
        should == (0...3).map { |i| "Cequel #{i}" }
    end

    it 'should query Time range for Timeuuid key' do
      PublishedPost['cassandra'].before(now - 1.minute).map(&:permalink).
        should == %w(cequel0 cequel1 cequel2)
    end

    it 'should cast argument' do
      Post['cassandra'].before('cequel3'.force_encoding('ASCII-8BIT')).
        map(&:title).should == (0...3).map { |i| "Cequel #{i}" }
    end
  end

  describe '#upto' do
    let(:records) { [posts, published_posts] }

    it 'should return collection up to given key' do
      Post['cassandra'].upto('cequel3').map(&:title).
        should == (0..3).map { |i| "Cequel #{i}" }
    end

    it 'should cast argument' do
      Post['cassandra'].upto('cequel3'.force_encoding('ASCII-8BIT')).
        map(&:title).should == (0..3).map { |i| "Cequel #{i}" }
    end

    it 'should query Time range for Timeuuid key' do
      PublishedPost['cassandra'].upto(now - 1.minute).map(&:permalink).
        should == %w(cequel0 cequel1 cequel2 cequel3)
    end
  end

  describe '#in' do
    let(:records) { [posts, published_posts] }

    it 'should return collection with inclusive upper bound' do
      Post['cassandra'].in('cequel1'..'cequel3').map(&:title).
        should == (1..3).map { |i| "Cequel #{i}" }
    end

    it 'should cast arguments' do
      Post['cassandra'].in('cequel1'.force_encoding('ASCII-8BIT')..
                              'cequel3'.force_encoding('ASCII-8BIT')).
        map(&:title).should == (1..3).map { |i| "Cequel #{i}" }
    end

    it 'should return collection with exclusive upper bound' do
      Post['cassandra'].in('cequel1'...'cequel3').map(&:title).
        should == (1...3).map { |i| "Cequel #{i}" }
    end

    it 'should query Time range for Timeuuid key' do
      PublishedPost['cassandra'].in((now - 3.minutes)..(now - 1.minute)).
        map(&:permalink).should == %w(cequel1 cequel2 cequel3)
    end

    it 'should query Time range for Timeuuid key with exclusive upper bound' do
      PublishedPost['cassandra'].in((now - 3.minutes)...(now - 1.minute)).
        map(&:permalink).should == %w(cequel1 cequel2)
    end
  end

  describe '#reverse' do
    let(:records) { [posts, comments] }

    it 'should not call the database' do
      disallow_queries!
      Post['cassandra'].reverse
    end

    it 'should return collection in reverse' do
      Post['cassandra'].reverse.map(&:title).
        should == (0...5).map { |i| "Cequel #{i}" }.reverse
    end

    it 'should batch iterate over collection in reverse' do
      Post['cassandra'].reverse.find_each(:batch_size => 2).map(&:title).
        should == (0...5).map { |i| "Cequel #{i}" }.reverse
    end

    it 'should raise an error if range key is a partition key' do
      expect { Post.all.reverse }.to raise_error(Cequel::Record::IllegalQuery)
    end

    it 'should use the correct ordering column in deeply nested models' do
      Comment['cassandra']['cequel0'].reverse.map(&:body).
        should == (0...5).map { |i| "Comment #{i}" }.reverse
    end
  end

  describe 'last' do
    it 'should return the last instance' do
      Post['cassandra'].last.title.should == "Cequel 4"
    end

    it 'should return the last N instances if specified' do
      Post['cassandra'].last(3).map(&:title).
        should == ["Cequel 2", "Cequel 3", "Cequel 4"]
    end
  end

  describe '#first' do
    let(:records) { blogs }

    context 'with no arguments' do
      it 'should return an arbitrary record' do
        subdomains.should include(Blog.first.subdomain)
      end
    end

    context 'with a given size' do
      subject { Blog.first(2) }

      it { should be_a(Array) }
      it { should have(2).items }
      specify { (subject.map(&:subdomain) & subdomains).should have(2).items }
    end
  end

  describe '#limit' do
    let(:records) { blogs }

    it 'should return the number of records requested' do
      Blog.limit(2).should have(2).entries
    end
  end

  describe '#select' do
    let(:records) { blogs }

    context 'with no block' do
      subject { Blog.select(:subdomain, :name).first }

      it { should be_loaded(:name) }
      it { should_not be_loaded(:description) }
      specify { expect { subject.name }.to_not raise_error }
      specify { expect { subject.description }.
        to raise_error(Cequel::Record::MissingAttributeError) }
    end

    context 'with block' do
      it 'should delegate to the Enumerable method' do
        Blog.all.select { |p| p.subdomain[/\d+/].to_i.even? }.
          map(&:subdomain).should =~ %w(blog-0 blog-2)
      end
    end
  end

  describe '#where' do
    it 'should correctly query for secondary indexed columns' do
      Post.where(:author_id, uuids.first).map(&:permalink).
        should == %w(cequel0 cequel2 cequel4)
    end

    it 'should cast argument for column' do
      Post.where(:author_id, uuids.first.to_s).map(&:permalink).
        should == %w(cequel0 cequel2 cequel4)
    end

    it 'should raise ArgumentError if column is not recognized' do
      expect { Post.where(:bogus, 'Business') }.
        to raise_error(ArgumentError)
    end

    it 'should raise ArgumentError if column is not indexed' do
      expect { Post.where(:title, 'Cequel 0') }.
        to raise_error(ArgumentError)
    end

    it 'should raise ArgumentError if column is a key' do
      expect { Post.where(:permalink, 'cequel0') }.
        to raise_error(ArgumentError)
    end

    it 'should raise IllegalQuery if applied twice' do
      expect { Post.where(:author_id, uuids.first).
        where(:author_name, 'Mat Brown') }.
        to raise_error(Cequel::Record::IllegalQuery)
    end
  end

  describe '#count' do
    let(:records) { blogs }

    it 'should count records' do
      Blog.count.should == 3
    end
  end

  describe 'scope methods' do
    it 'should delegate unknown methods to class singleton with current scope' do
      Post['cassandra'].latest(3).map(&:permalink).
        should == %w(cequel4 cequel3 cequel2)
    end

    it 'should raise NoMethodError if undefined method called' do
      expect { Post['cassandra'].bogus }.to raise_error(NoMethodError)
    end
  end

  describe '#update_all' do
    let(:records) { posts }

    it 'should be able to update with no scoping' do
      Post.update_all(title: 'Same Title')
      Post.all.map(&:title).should == Array.new(posts.length) { 'Same Title' }
    end

    it 'should update posts with scoping' do
      Post['cassandra'].update_all(title: 'Same Title')
      Post['cassandra'].map(&:title).
        should == Array.new(cassandra_posts.length) { 'Same Title' }
      Post['postgres'].map(&:title).should == postgres_posts.map(&:title)
    end

    it 'should update fully specified collection' do
      Post['cassandra']['cequel0', 'cequel1', 'cequel2'].
        update_all(title: 'Same Title')
      Post['cassandra']['cequel0', 'cequel1', 'cequel2'].map(&:title).
        should == Array.new(3) { 'Same Title' }
      Post['cassandra']['cequel3', 'cequel4'].map(&:title).
        should == cassandra_posts.drop(3).map(&:title)
    end
  end

  describe '#delete_all' do
    let(:records) { posts }

    it 'should be able to delete with no scoping' do
      Post.delete_all
      Post.count.should be_zero
    end

    it 'should be able to delete with scoping' do
      Post['postgres'].delete_all
      Post['postgres'].count.should be_zero
      Post['cassandra'].count.should == cassandra_posts.length
    end

    it 'should be able to delete fully specified collection' do
      Post['postgres']['sequel0', 'sequel1'].delete_all
      Post['postgres'].map(&:permalink).
        should == postgres_posts.drop(2).map(&:permalink)
    end
  end

  describe '#destroy_all' do
    let(:records) { posts }

    it 'should be able to delete with no scoping' do
      Post.destroy_all
      Post.count.should be_zero
    end

    it 'should be able to delete with scoping' do
      Post['postgres'].destroy_all
      Post['postgres'].count.should be_zero
      Post['cassandra'].count.should == cassandra_posts.length
    end

    it 'should be able to delete fully specified collection' do
      Post['postgres']['sequel0', 'sequel1'].destroy_all
      Post['postgres'].map(&:permalink).
        should == postgres_posts.drop(2).map(&:permalink)
    end
  end
end
