require_relative 'spec_helper'

describe Cequel::Model::Validations do
  model :Post do
    key :permalink, :text
    column :title, :text
    column :body, :text

    validates :title, :presence => true
  end

  let(:invalid_post) do
    Post.new do |post|
      post.permalink = 'invalid'
      post.body = 'This is an invalid post.'
    end
  end
  let(:valid_post) do
    Post.new do |post|
      post.permalink = 'valid'
      post.title = 'Valid Post'
    end
  end
  let(:unloaded_post) { Post['unloaded'] }

  describe '#valid?' do
    it 'should be false if model is not valid' do
      invalid_post.should_not be_valid
    end

    it 'should be true if model is valid' do
      valid_post.should be_valid
    end
  end

  describe '#save' do
    it 'should return false and not persist model if invalid' do
      invalid_post.save.should be_false
    end

    it 'should return true and persist model if valid' do
      valid_post.save.should be_true
      Post.find('valid').title.should == 'Valid Post'
    end

    it 'should bypass validations if :validate => false is passed' do
      invalid_post.save(:validate => false).should be_true
      Post.find('invalid').body.should == 'This is an invalid post.'
    end
  end

  describe '#save!' do
    it 'should raise error and not persist model if invalid' do
      expect { invalid_post.save!  }.
        to raise_error(Cequel::Model::RecordInvalid)
    end

    it 'should persist model and return self if valid' do
      expect { valid_post.save! }.to_not raise_error
      Post.find(valid_post.permalink).title.should == 'Valid Post'
    end
  end

  describe '#update_attributes!' do
    before { pending 'update_attributes' }
    let(:post) do
      connection.stub(:execute).with("SELECT * FROM posts WHERE ? = ? LIMIT 1", :id, 1).
        and_return result_stub(:id => 1, :blog_id => 1, :title => 'Cequel')
      Post.find(1)
    end

    it 'should change attributes and save them if valid' do
      connection.should_receive(:execute).
        with "UPDATE posts SET ? = ? WHERE ? = ?", 'body', 'Cequel cequel', :id, 1
      post.update_attributes!(:body => 'Cequel cequel')
    end

    it 'should raise error if not valid' do
      post.require_title = true
      expect { post.update_attributes!(:title => nil) }.
        to raise_error(Cequel::Model::RecordInvalid)
    end
  end

  describe '::create!' do
    before { pending 'create' }
    it 'should raise RecordInvalid and not persist model if invalid' do
      expect do
        Post.create!(:id => 1, :body => 'Cequel')
      end.to raise_error(Cequel::Model::RecordInvalid)
    end

    it 'should and return model if valid' do
      connection.should_receive(:execute).
        with "INSERT INTO posts (?) VALUES (?)", ['id', 'title'], [1, 'Cequel']

      Post.create!(:id => 1, :title => 'Cequel').
        title.should == 'Cequel'
    end
  end

  describe 'callbacks' do
    before { pending 'callbacks' }

    it 'should call validation callbacks' do
      post = Post.new(:id => 1)
      post.valid?
      post.should have_callback(:validation)
    end
  end
end
