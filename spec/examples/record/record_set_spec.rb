require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record::RecordSet do
  model :Blog do
    key :subdomain, :text
    column :name, :text
    column :description, :text
  end

  model :Post do
    key :blog_subdomain, :text
    key :permalink, :text
    column :title, :text
    column :body, :text
    column :author_id, :uuid, :index => true
    column :author_name, :text, :index => true
    list :tags, :text
    set :categories, :text
    map :shares, :text, :int
  end

  let(:subdomains) { [] }
  let(:uuids) { Array.new(2) { CassandraCQL::UUID.new }}

  before do
    cequel.batch do
      3.times do |i|
        Blog.new do |blog|
          subdomains << blog.subdomain = "blog-#{i}"
          blog.name = "Blog #{i}"
          blog.description = "This is Blog number #{i}"
        end.save
      end
    end
    cequel.batch do
      5.times do |i|
        cequel[:posts].insert(
          :blog_subdomain => 'cassandra',
          :permalink => "cequel#{i}",
          :title => "Cequel #{i}",
          :body => "Post number #{i}",
          :author_id => uuids[i%2]
        )
        cequel[:posts].insert(
          :blog_subdomain => 'postgres',
          :permalink => "sequel#{i}",
          :title => "Sequel #{i}"
        )
      end
    end
  end

  describe '::find' do
    context 'simple primary key' do
      subject { Blog.find('blog-0') }

      its(:subdomain) { should == 'blog-0' }
      its(:name) { should == 'Blog 0' }

      it { should be_persisted }
      it { should_not be_transient }
      specify { Blog.new.should_not be_persisted }
      specify { Blog.new.should be_transient }

      specify do
        expect { Blog.find('bogus') }.
          to raise_error(Cequel::Record::RecordNotFound)
      end
    end

    context 'compound primary key' do
      subject { Post['cassandra'].find('cequel0') }

      its(:blog_subdomain) { should == 'cassandra' }
      its(:permalink) { should == 'cequel0' }
      its(:title) { should == 'Cequel 0' }

      it { should be_persisted }
      it { should_not be_transient }
      specify { Post.new.should_not be_persisted }
      specify { Post.new.should be_transient }

      specify do
        expect { Post['cequel'].find('bogus')}.
          to raise_error(Cequel::Record::RecordNotFound)
      end
    end
  end

  describe '::[]' do
    context 'simple primary key' do
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
    end

    context 'compound primary key' do
      subject { Post['cassandra']['cequel0'] }

      it 'should not query the database' do
        expect(cequel).not_to receive(:execute)
        subject.blog_subdomain.should == 'cassandra'
        subject.permalink.should == 'cequel0'
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
  end

  describe '#all' do
    it 'should return all the records' do
      Blog.all.map(&:subdomain).should =~ subdomains
    end
  end

  describe '#find_each' do
    it 'should respect :batch_size argument' do
      cequel.should_receive(:execute).twice.and_call_original
      Blog.find_each(:batch_size => 2).map(&:subdomain).
        should =~ subdomains
    end
    it 'should iterate over all keys' do
      Post.find_each(:batch_size => 2).map(&:title).
        should =~ (0...5).flat_map { |i| ["Cequel #{i}", "Sequel #{i}"] }
    end
  end

  describe '#at' do
    it 'should return partial collection' do
      Post.at('cassandra').find_each(:batch_size => 2).map(&:title).
        should == (0...5).map { |i| "Cequel #{i}" }
    end
  end

  describe '#[]' do
    it 'should create partial collection if not all keys specified' do
      Post['cassandra'].find_each(:batch_size => 2).map(&:title).
        should == (0...5).map { |i| "Cequel #{i}" }
    end
  end

  describe '#/' do
    it 'should behave like #at' do
      (Post / 'cassandra').find_each(:batch_size => 2).map(&:title).
        should == (0...5).map { |i| "Cequel #{i}" }
    end
  end

  describe '#after' do
    it 'should return collection after given key' do
      Post.at('cassandra').after('cequel1').map(&:title).
        should == (2...5).map { |i| "Cequel #{i}" }
    end
  end

  describe '#from' do
    it 'should return collection starting with given key' do
      Post.at('cassandra').from('cequel1').map(&:title).
        should == (1...5).map { |i| "Cequel #{i}" }
    end

    it 'should raise ArgumentError when called on partition key' do
      expect { Post.from('cassandra') }.
        to raise_error(Cequel::Model::IllegalQuery)
    end
  end

  describe '#before' do
    it 'should return collection before given key' do
      Post.at('cassandra').before('cequel3').map(&:title).
        should == (0...3).map { |i| "Cequel #{i}" }
    end
  end

  describe '#upto' do
    it 'should return collection up to given key' do
      Post.at('cassandra').upto('cequel3').map(&:title).
        should == (0..3).map { |i| "Cequel #{i}" }
    end
  end

  describe '#in' do
    it 'should return collection with inclusive upper bound' do
      Post.at('cassandra').in('cequel1'..'cequel3').map(&:title).
        should == (1..3).map { |i| "Cequel #{i}" }
    end

    it 'should return collection with exclusive upper bound' do
      Post.at('cassandra').in('cequel1'...'cequel3').map(&:title).
        should == (1...3).map { |i| "Cequel #{i}" }
    end
  end

  describe '#reverse' do
    it 'should not call the database' do
      disallow_queries!
      Post.at('cassandra').reverse
    end

    it 'should return collection in reverse' do
      Post.at('cassandra').reverse.map(&:title).
        should == (0...5).map { |i| "Cequel #{i}" }.reverse
    end

    it 'should batch iterate over collection in reverse' do
      Post.at('cassandra').reverse.find_each(:batch_size => 2).map(&:title).
        should == (0...5).map { |i| "Cequel #{i}" }.reverse
    end

    it 'should raise an error if range key is a partition key' do
      expect { Post.all.reverse }.to raise_error(Cequel::Model::IllegalQuery)
    end
  end

  describe 'last' do
    it 'should return the last instance' do
      Post.at('cassandra').last.title.should == "Cequel 4"
    end

    it 'should return the last N instances if specified' do
      Post.at('cassandra').last(3).map(&:title).
        should == ["Cequel 2", "Cequel 3", "Cequel 4"]
    end
  end

  describe '#first' do
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
    it 'should return the number of blogs requested' do
      Blog.limit(2).should have(2).entries
    end
  end

  describe '#select' do
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
    it 'should count records' do
      Blog.count.should == 3
    end
  end

end
