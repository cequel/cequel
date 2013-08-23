require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Scope do
  model :Post do
    key :permalink, :text
    column :title, :text
    column :body, :text
  end

  let(:permalinks) { [] }

  before do
    cequel.batch do
      3.times do |i|
        Post.new do |post|
          permalinks << post.permalink = "post-#{i}"
          post.title = "Post #{i}"
          post.body = "This is Post number #{i}"
        end.save
      end
    end
  end

  describe '#all' do
    it 'should return all the records' do
      Post.all.map(&:permalink).should =~ permalinks
    end
  end

  describe '#find_each' do
    it 'should respect :batch_size argument' do
      cequel.should_receive(:execute).twice.and_call_original
      Post.find_each(:batch_size => 2).map(&:permalink).
        should =~ permalinks
    end
  end

  describe '#first' do
    context 'with no arguments' do
      it 'should return an arbitrary record' do
        permalinks.should include(Post.first.permalink)
      end
    end

    context 'with a given size' do
      subject { Post.first(2) }

      it { should be_a(Array) }
      it { should have(2).items }
      specify { (subject.map(&:permalink) & permalinks).should have(2).items }
    end
  end

  describe '#limit' do
    it 'should return the number of posts requested' do
      Post.limit(2).should have(2).entries
    end
  end

  describe '#select' do
    context 'with no block' do
      subject { Post.select(:permalink, :title).first }

      it { should be_loaded(:title) }
      it { should_not be_loaded(:body) }
      specify { expect { subject.title }.to_not raise_error }
      specify { expect { subject.body }.
        to raise_error(Cequel::Model::MissingAttributeError) }
    end

    context 'with block' do
      it 'should delegate to the Enumerable method' do
        Post.all.select { |p| p.permalink[/\d+/].to_i.even? }.
          map(&:permalink).should =~ %w(post-0 post-2)
      end
    end
  end

  describe '#count' do
    it 'should count records' do
      Post.count.should == 3
    end
  end

  describe '#reverse' do
    specify { pending 'compound keys' }
  end

  describe '#last' do
    specify { pending 'compound keys' }
  end

end
