# -*- encoding : utf-8 -*-
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
    key :published_at, :timeuuid, order: :desc
    column :permalink, :ascii, index: true
  end

  let(:subdomains) { blogs.map(&:subdomain) }
  let(:uuids) { Array.new(2) { Cequel.uuid }}
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
        :id => Cequel.uuid(Time.now - 5 + i),
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

      it { is_expected.to be_persisted }
      it { is_expected.not_to be_transient }

      it 'should cast argument to correct type' do
        expect(Blog.find('blog-0'.force_encoding('ASCII-8BIT'))).to eq(blogs.first)
      end

      it 'should return multiple results as an array from vararg keys' do
        expect(Blog.find('blog-0', 'blog-1')).to eq(blogs.first(2))
      end

      it 'should return multiple results as an array from array of keys' do
        expect(Blog.find(['blog-0', 'blog-1'])).to eq(blogs.first(2))
      end

      it 'should return result in an array from one-element array of keys' do
        expect(Blog.find(['blog-1'])).to eq([blogs[1]])
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

      it { is_expected.to be_persisted }
      it { is_expected.not_to be_transient }
      specify { expect(Post.new).not_to be_persisted }
      specify { expect(Post.new).to be_transient }

      it 'should cast all keys to correct type' do
        expect(Post['cassandra'.force_encoding('ASCII-8BIT')].
          find('cequel0'.force_encoding('ASCII-8BIT'))).to be
      end

      it 'should raise RecordNotFound if bad argument passed' do
        expect { Post['cequel'].find('bogus')}.
          to raise_error(Cequel::Record::RecordNotFound)
      end

      it 'should take vararg of values for single key' do
        expect(Post.find('cassandra', 'cequel0')).to eq(posts.first)
      end

      it 'should take multiple values for key' do
        expect(Post.find('cassandra', ['cequel0', 'cequel1'])).to eq(posts.first(2))
      end

      it 'should use Enumerable#find if block given' do
        expect(Post['cassandra'].find { |post| post.title.include?('1') })
          .to eq(posts[1])
      end

      it 'should raise error if not enough key values specified' do
        expect { Post.find('cassandra') }.to raise_error(ArgumentError)
      end
    end
  end

  describe '::[]' do
    context 'fully specified simple primary key' do
      let(:records) { blogs }
      subject { Blog['blog-0'] }

      it 'should not query the database' do
        disallow_queries!
        expect(subject.subdomain).to eq('blog-0')
      end

      it 'should lazily query the database when attribute accessed' do
        expect(subject.name).to eq('Blog 0')
      end

      it 'should get all eager-loadable attributes on first lazy load' do
        subject.name
        disallow_queries!
        expect(subject.description).to eq('This is Blog number 0')
      end

      it 'should cast argument' do
        expect(subject.subdomain.encoding.name).to eq('US-ASCII')
      end
    end

    context 'fully specified compound primary key' do
      let(:records) { posts }
      subject { Post['cassandra']['cequel0'] }

      it 'should not query the database' do
        expect(cequel).not_to receive(:execute)
        expect(subject.blog_subdomain).to eq('cassandra')
        expect(subject.permalink).to eq('cequel0')
      end

      it 'should cast all keys to the correct type' do
        expect(subject.blog_subdomain.encoding.name).to eq('US-ASCII')
        expect(subject.permalink.encoding.name).to eq('US-ASCII')
      end

      it 'should lazily query the database when attribute accessed' do
        expect(subject.title).to eq('Cequel 0')
      end

      it 'should get all eager-loadable attributes on first lazy load' do
        subject.title
        expect(cequel).not_to receive(:execute)
        expect(subject.body).to eq('Post number 0')
      end
    end

    context 'partially specified compound primary key' do
      let(:records) { posts }
      it 'should create partial collection if not all keys specified' do
        expect(Post['cassandra'].find_each(:batch_size => 2).map(&:title)).
          to eq((0...5).map { |i| "Cequel #{i}" })
      end
    end
  end

  describe '#values_at' do
    context 'multiple simple primary keys' do
      let(:records) { blogs }
      subject { Blog.values_at('blog-0', 'blog-1') }

      it 'should return both specified records' do
        expect(subject.map(&:subdomain)).to match_array(%w(blog-0 blog-1))
      end

      it 'should not query the database' do
        disallow_queries!
        subject.map(&:subdomain)
      end

      it 'should load value lazily' do
        expect(subject.first.name).to eq('Blog 0')
      end

      it 'should load values for all referenced records on first access' do
        expect_statement_count 1 do
          expect(subject.first.name).to eq('Blog 0')
          expect(subject.last.name).to eq('Blog 1')
        end
      end
    end

    context 'partially specified compound primary key with multiple partition keys' do
      let(:records) { posts }
      subject { Post.values_at('cassandra', 'postgres') }

      it 'should return scope to keys' do
        expect(subject.map { |post| post.title }).to match_array((0...5).
          map { |i| ["Cequel #{i}", "Sequel #{i}"] }.flatten)
      end
    end

    context 'fully specified compound primary key with multiple partition keys' do
      let(:records) { [posts, orm_posts] }

      subject { Post.values_at('cassandra', 'orms')['cequel0'] }

      it 'should return collection of unloaded models' do
        disallow_queries!
        expect(subject.map(&:key_values)).
          to eq([['cassandra', 'cequel0'], ['orms', 'cequel0']])
      end

      it 'should lazy-load all records when properties of one accessed' do
        expect_statement_count 1 do
          expect(subject.first.title).to eq('Cequel 0')
          expect(subject.second.title).to eq('Cequel ORM 0')
        end
      end
    end

    context 'fully specified compound primary key with multiple clustering columns' do
      let(:records) { posts }
      subject { Post['cassandra'].values_at('cequel0', 'cequel1') }

      it 'should combine partition key with each clustering column' do
        disallow_queries!
        expect(subject.map(&:key_values)).
          to eq([['cassandra', 'cequel0'], ['cassandra', 'cequel1']])
      end

      it 'should lazily load all records when one record accessed' do
        expect_statement_count 1 do
          expect(subject.first.title).to eq('Cequel 0')
          expect(subject.second.title).to eq('Cequel 1')
        end
      end

      it 'should not allow collection columns to be selected' do
        expect { Post.select(:tags)['cassandra'].values_at('cequel0', 'cequel1') }.
          to raise_error(ArgumentError)
      end
    end

    context 'non-final clustering column' do
      let(:records) { [] }

      it 'should raise IllegalQuery' do
        disallow_queries!
        expect { Comment['cassandra'].values_at('cequel0') }.
          to raise_error(Cequel::Record::IllegalQuery)
      end
    end
  end

  describe '#all' do
    let(:records) { blogs }

    it 'should return all the records' do
      expect(Blog.all.map(&:subdomain)).to match_array(subdomains)
    end
  end

  describe '#find_each' do
    let(:records) { [posts, blogs, mongo_posts] }

    it 'should respect :batch_size argument' do
      expect_statement_count 2 do
        expect(Blog.find_each(:batch_size => 2).map(&:subdomain)).
          to match_array(subdomains)
      end
    end

    it 'should iterate over all keys' do
      expect(Post.find_each(:batch_size => 2).map(&:title)).to match_array(
        (0...5).flat_map { |i| ["Cequel #{i}", "Sequel #{i}", "Mongoid #{i}"] }
      )
    end

    describe "hydration" do
      subject { x = nil
        Post.find_each(:batch_size => 2){|it| x = it; break}
        x
      }

      it 'should hydrate empty lists properly' do
        expect(subject.tags).to eq []
      end

      it 'should hydrate empty sets properly' do
        expect(subject.categories).to eq ::Set[]
      end

      it 'should hydrate empty maps properly' do
        expect(subject.shares).to eq Hash.new
      end
    end
  end

  describe '#find_in_batches' do
    let!(:records) { [posts, blogs, mongo_posts] }

    it 'should respect :batch_size argument' do
      expect_statement_count 2 do
        Blog.find_in_batches(:batch_size => 2){|a_batch| next }
      end
    end

    it 'should iterate over all keys' do
      expected_posts = (posts + mongo_posts)
      found_posts = []

      Post.find_in_batches(:batch_size => 2) {|recs|
        expect(recs).to be_kind_of Array
        found_posts += recs
      }
      expect(found_posts).to include(*expected_posts)
      expect(found_posts.length).to eq(expected_posts.size)
    end

    it 'should iterate over batches' do
      expected_posts = (posts + mongo_posts)

      expect{|blk| Post.find_in_batches(:batch_size => 2, &blk)}
        .to yield_control.at_least(expected_posts.size / 2).times
    end


    describe "hydration" do
      subject { x = nil
        Post.find_each(:batch_size => 2){|it| x = it; break}
        x
      }

      it 'should hydrate empty lists properly' do
        expect(subject.tags).to eq []
      end

      it 'should hydrate empty sets properly' do
        expect(subject.categories).to eq ::Set[]
      end

      it 'should hydrate empty maps properly' do
        expect(subject.shares).to eq Hash.new
      end
    end
  end

  describe '#find' do
  end

  describe '#[]' do
    it 'should return partial collection' do
      expect(Post['cassandra'].find_each(:batch_size => 2).map(&:title)).
        to eq((0...5).map { |i| "Cequel #{i}" })
    end

    it 'should cast arguments correctly' do
      expect(Post['cassandra'.force_encoding('ASCII-8BIT')].
        find_each(:batch_size => 2).map(&:title)).
        to eq((0...5).map { |i| "Cequel #{i}" })
    end
  end

  describe '#/' do
    it 'should behave like #[]' do
      expect((Post / 'cassandra').find_each(:batch_size => 2).map(&:title)).
        to eq((0...5).map { |i| "Cequel #{i}" })
    end
  end

  describe '#after' do
    let(:records) { [posts, published_posts] }

    it 'should return collection after given key' do
      expect(Post['cassandra'].after('cequel1').map(&:title)).
        to eq((2...5).map { |i| "Cequel #{i}" })
    end

    it 'should cast argument' do
      expect(Post['cassandra'].after('cequel1'.force_encoding('ASCII-8BIT')).
        map(&:title)).to eq((2...5).map { |i| "Cequel #{i}" })
    end

    it 'should query Time range for Timeuuid key' do
      expect(PublishedPost['cassandra'].after(now - 3.minutes).map(&:permalink)).
        to eq(%w(cequel4 cequel3 cequel2))
    end
  end

  describe '#from' do
    let(:records) { [posts, published_posts] }

    it 'should return collection starting with given key' do
      expect(Post['cassandra'].from('cequel1').map(&:title)).
        to eq((1...5).map { |i| "Cequel #{i}" })
    end

    it 'should cast argument' do
      expect(Post['cassandra'].from('cequel1'.force_encoding('ASCII-8BIT')).
        map(&:title)).to eq((1...5).map { |i| "Cequel #{i}" })
    end

    it 'should query Time range for Timeuuid key' do
      expect(PublishedPost['cassandra'].from(now - 3.minutes).map(&:permalink)).
        to eq(%w(cequel4 cequel3 cequel2 cequel1))
    end

    it 'should raise ArgumentError when called on partition key' do
      expect { Post.from('cassandra') }.
        to raise_error(Cequel::Record::IllegalQuery)
    end
  end

  describe '#before' do
    let(:records) { [posts, published_posts] }

    it 'should return collection before given key' do
      expect(Post['cassandra'].before('cequel3').map(&:title)).
        to eq((0...3).map { |i| "Cequel #{i}" })
    end

    it 'should query Time range for Timeuuid key' do
      expect(PublishedPost['cassandra'].before(now - 1.minute).map(&:permalink)).
        to eq(%w(cequel2 cequel1 cequel0))
    end

    it 'should cast argument' do
      expect(Post['cassandra'].before('cequel3'.force_encoding('ASCII-8BIT')).
        map(&:title)).to eq((0...3).map { |i| "Cequel #{i}" })
    end
  end

  describe '#upto' do
    let(:records) { [posts, published_posts] }

    it 'should return collection up to given key' do
      expect(Post['cassandra'].upto('cequel3').map(&:title)).
        to eq((0..3).map { |i| "Cequel #{i}" })
    end

    it 'should cast argument' do
      expect(Post['cassandra'].upto('cequel3'.force_encoding('ASCII-8BIT')).
        map(&:title)).to eq((0..3).map { |i| "Cequel #{i}" })
    end

    it 'should query Time range for Timeuuid key' do
      expect(PublishedPost['cassandra'].upto(now - 1.minute).map(&:permalink)).
        to eq(%w(cequel3 cequel2 cequel1 cequel0))
    end
  end

  describe '#in' do
    let(:records) { [posts, published_posts] }

    it 'should return collection with inclusive upper bound' do
      expect(Post['cassandra'].in('cequel1'..'cequel3').map(&:title)).
        to eq((1..3).map { |i| "Cequel #{i}" })
    end

    it 'should cast arguments' do
      expect(Post['cassandra'].in('cequel1'.force_encoding('ASCII-8BIT')..
                              'cequel3'.force_encoding('ASCII-8BIT')).
        map(&:title)).to eq((1..3).map { |i| "Cequel #{i}" })
    end

    it 'should return collection with exclusive upper bound' do
      expect(Post['cassandra'].in('cequel1'...'cequel3').map(&:title)).
        to eq((1...3).map { |i| "Cequel #{i}" })
    end

    it 'should query Time range for Timeuuid key' do
      expect(PublishedPost['cassandra'].in((now - 3.minutes)..(now - 1.minute)).
        map(&:permalink)).to eq(%w(cequel3 cequel2 cequel1))
    end

    it 'should query Time range for Timeuuid key with exclusive upper bound' do
      expect(PublishedPost['cassandra'].in((now - 3.minutes)...(now - 1.minute)).
        map(&:permalink)).to eq(%w(cequel2 cequel1))
    end
  end

  describe '#reverse' do
    let(:records) { [published_posts, comments] }

    it 'should not call the database' do
      disallow_queries!
      PublishedPost['cassandra'].reverse
    end

    it 'should return collection in reverse' do
      expect(PublishedPost['cassandra'].reverse.map(&:permalink)).
        to eq((0...5).map { |i| "cequel#{i}" })
    end

    it 'should batch iterate over collection in reverse' do
      expect(PublishedPost['cassandra'].reverse.find_each(:batch_size => 2).map(&:permalink)).
        to eq((0...5).map { |i| "cequel#{i}" })
    end

    it 'should raise an error if range key is a partition key' do
      expect { PublishedPost.all.reverse }.to raise_error(Cequel::Record::IllegalQuery)
    end

    it 'should use the correct ordering column in deeply nested models' do
      expect(Comment['cassandra']['cequel0'].reverse.map(&:body)).
        to eq((0...5).map { |i| "Comment #{i}" }.reverse)
    end
  end

  describe 'last' do
    it 'should return the last instance' do
      expect(Post['cassandra'].last.title).to eq("Cequel 4")
    end

    it 'should return the last N instances if specified' do
      expect(Post['cassandra'].last(3).map(&:title)).
        to eq(["Cequel 2", "Cequel 3", "Cequel 4"])
    end
  end

  describe '#first' do
    let(:records) { blogs }

    context 'with no arguments' do
      it 'should return an arbitrary record' do
        expect(subdomains).to include(Blog.first.subdomain)
      end
    end

    context 'with a given size' do
      subject { Blog.first(2) }

      it { is_expected.to be_a(Array) }
      its(:size) { should be(2) }
      specify { expect((subject.map(&:subdomain) & subdomains).size).to be(2) }
    end
  end

  describe '#limit' do
    let(:records) { blogs }

    it 'should return the number of records requested' do
      expect(Blog.limit(2).length).to be(2)
    end
  end

  describe '#select' do
    let(:records) { blogs }

    context 'with no block' do
      subject { Blog.select(:subdomain, :name).first }

      it { is_expected.to be_loaded(:name) }
      it { is_expected.not_to be_loaded(:description) }
      specify { expect { subject.name }.to_not raise_error }
      specify { expect { subject.description }.
        to raise_error(Cequel::Record::MissingAttributeError) }
    end

    context 'with block' do
      it 'should delegate to the Enumerable method' do
        expect(Blog.all.select { |p| p.subdomain[/\d+/].to_i.even? }.
          map(&:subdomain)).to match_array(%w(blog-0 blog-2))
      end
    end
  end

  describe '#where' do
    context 'simple primary key' do
      let(:records) { blogs }

      it 'should correctly query for simple primary key with two arguments' do
        expect(Blog.where(:subdomain, 'blog-0'))
          .to eq(blogs.first(1))
      end

      it 'should correctly query for simple primary key with hash argument' do
        expect(Blog.where(subdomain: 'blog-0'))
          .to eq(blogs.first(1))
      end
    end

    context 'compound primary key' do
      it 'should correctly query for first primary key column' do
        expect(Post.where(blog_subdomain: 'cassandra'))
          .to eq(cassandra_posts)
      end

      it 'should perform IN query when passed multiple values' do
        expect(Post.where(blog_subdomain: %w(cassandra postgres)))
          .to match_array(cassandra_posts + postgres_posts)
      end

      it 'should correctly query for both primary key columns' do
        expect(Post.where(blog_subdomain: 'cassandra', permalink: 'cequel0'))
          .to eq(cassandra_posts.first(1))
      end

      it 'should correctly query for both primary key columns chained' do
        expect(Post.where(blog_subdomain: 'cassandra')
               .where(permalink: 'cequel0'))
          .to eq(cassandra_posts.first(1))
      end

      it 'should perform range query when passed range' do
        expect(Post.where(blog_subdomain: %w(cassandra),
                          permalink: 'cequel0'..'cequel2'))
          .to eq(cassandra_posts.first(3))
      end

      it 'should raise error if lower-order primary key specified without higher' do
        expect { Post.where(permalink: 'cequel0').first }
          .to raise_error(Cequel::Record::IllegalQuery)
      end
    end

    context 'secondary indexed column' do
      it 'should query for secondary indexed columns with two arguments' do
        expect(Post.where(:author_id, uuids.first).map(&:permalink)).
          to eq(%w(cequel0 cequel2 cequel4))
      end

      it 'should query for secondary indexed columns with hash argument' do
        expect(Post.where(author_id: uuids.first).map(&:permalink)).
          to eq(%w(cequel0 cequel2 cequel4))
      end

      it 'should not allow multiple columns in the arguments' do
        expect { Post.where(author_id: uuids.first, author_name: 'Mat Brown') }
          .to raise_error(Cequel::Record::IllegalQuery)
      end

      it 'should not allow chaining of multiple columns' do
        expect { Post.where(:author_id, uuids.first).
          where(:author_name, 'Mat Brown') }.
          to raise_error(Cequel::Record::IllegalQuery)
      end

      it 'should cast argument for column' do
        expect(Post.where(:author_id, uuids.first.to_s).map(&:permalink)).
          to eq(%w(cequel0 cequel2 cequel4))
      end
    end

    context 'mixing keys and secondary-indexed columns' do
      it 'should allow mixture in hash argument' do
        expect(Post.where(blog_subdomain: 'cassandra', author_id: uuids.first).
          entries.length).to be(3)
      end

      it 'should allow mixture in chain with primary first' do
        expect(Post.where(blog_subdomain: 'cassandra')
          .where(author_id: uuids.first).entries.length).to be(3)
      end

      it 'should allow mixture in chain with secondary first' do
        expect(Post.where(author_id: uuids.first)
          .where(blog_subdomain: 'cassandra').entries.length).to be(3)
      end
    end

    context 'nonexistent column' do
      it 'should raise ArgumentError if column is not recognized' do
        expect { Post.where(:bogus, 'Business') }.
          to raise_error(ArgumentError)
      end
    end

    context 'non-indexed column' do
      it 'should raise ArgumentError if column is not indexed' do
        expect { Post.where(:title, 'Cequel 0') }.
          to raise_error(ArgumentError)
      end
    end
  end

  describe '#consistency' do
    it 'should perform query with specified consistency' do
      expect_query_with_consistency(/SELECT/, :one) do
        Post.consistency(:one).to_a
      end
    end
  end

  describe '#count' do
    let(:records) { blogs }

    it 'should count records' do
      expect(Blog.count).to eq(3)
    end
  end

  describe 'scope methods' do
    it 'should delegate unknown methods to class singleton with current scope' do
      expect(Post['cassandra'].latest(3).map(&:permalink)).
        to eq(%w(cequel4 cequel3 cequel2))
    end

    it 'should raise NoMethodError if undefined method called' do
      expect { Post['cassandra'].bogus }.to raise_error(NoMethodError)
    end
  end

  describe '#update_all' do
    let(:records) { posts }

    it 'should be able to update with no scoping' do
      Post.update_all(title: 'Same Title')
      expect(Post.all.map(&:title)).to eq(Array.new(posts.length) { 'Same Title' })
    end

    it 'should update posts with scoping' do
      Post['cassandra'].update_all(title: 'Same Title')
      expect(Post['cassandra'].map(&:title)).
        to eq(Array.new(cassandra_posts.length) { 'Same Title' })
      expect(Post['postgres'].map(&:title)).to eq(postgres_posts.map(&:title))
    end

    it 'should update fully specified collection' do
      Post['cassandra'].values_at('cequel0', 'cequel1', 'cequel2').
        update_all(title: 'Same Title')
      expect(Post['cassandra'].values_at('cequel0', 'cequel1', 'cequel2').map(&:title)).
        to eq(Array.new(3) { 'Same Title' })
      expect(Post['cassandra'].values_at('cequel3', 'cequel4').map(&:title)).
        to eq(cassandra_posts.drop(3).map(&:title))
    end
  end

  describe '#delete_all' do
    let(:records) { posts }

    it 'should be able to delete with no scoping' do
      Post.delete_all
      expect(Post.count).to be_zero
    end

    it 'should be able to delete with scoping' do
      Post['postgres'].delete_all
      expect(Post['postgres'].count).to be_zero
      expect(Post['cassandra'].count).to eq(cassandra_posts.length)
    end

    it 'should be able to delete fully specified collection' do
      Post['postgres'].values_at('sequel0', 'sequel1').delete_all
      expect(Post['postgres'].map(&:permalink)).
        to eq(postgres_posts.drop(2).map(&:permalink))
    end
  end

  describe '#destroy_all' do
    let(:records) { posts }

    it 'should be able to delete with no scoping' do
      Post.destroy_all
      expect(Post.count).to be_zero
    end

    it 'should be able to delete with scoping' do
      Post['postgres'].destroy_all
      expect(Post['postgres'].count).to be_zero
      expect(Post['cassandra'].count).to eq(cassandra_posts.length)
    end

    it 'should be able to delete fully specified collection' do
      Post['postgres'].values_at('sequel0', 'sequel1').destroy_all
      expect(Post['postgres'].map(&:permalink)).
        to eq(postgres_posts.drop(2).map(&:permalink))
    end
  end

  context "table clustered on time and uuid" do
    model(:BlogView) do
      key :blog_subdomain, :ascii
      key :view_time, :timestamp
      key :sk, :uuid, auto: true
    end

    let!(:blog_1_views){
      4.times
        .map{|i| BlogView.create!(blog_subdomain: "blog-1",
                                  view_time: now - i.minutes) }
        .sort_by(&:view_time)
    }

    it "can execute multi-batch queries with range on partial cluster key" do
      expect{|blk| BlogView['blog-1']
          .in( blog_1_views.first.view_time .. now )
          .find_each(batch_size: 2, &blk)}
        .to yield_successive_args *blog_1_views
    end

    it "can execute multi-batch queries with range on partial key excluding beginning of partition" do
      expect{|blk| BlogView['blog-1']
          .in( blog_1_views.second.view_time .. now )
          .find_each(batch_size: 2, &blk)}
        .to yield_successive_args *blog_1_views.drop(1)
    end

    it "can execute multi-batch queries with range on partial key excluding end of partition" do
      expect{|blk| BlogView['blog-1']
          .in( blog_1_views.first.view_time .. blog_1_views[-2].view_time )
          .find_each(batch_size: 2, &blk)}
        .to yield_successive_args *blog_1_views[0..-2]
    end

    it "can execute queries with range on partial cluster key" do
      expect{|blk| BlogView['blog-1']
          .in( blog_1_views.first.view_time .. now )
          .find_each(&blk)}
        .to yield_successive_args *blog_1_views
    end
  end

end
