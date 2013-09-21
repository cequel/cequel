require_relative 'spec_helper'

describe Cequel::Record::Validations do
  model :Post do
    key :permalink, :text
    column :title, :text
    column :body, :text

    validates :title, :presence => true
    before_validation { |post| post.called_validate_callback = true }

    attr_accessor :called_validate_callback
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
        to raise_error(Cequel::Record::RecordInvalid)
    end

    it 'should persist model and return self if valid' do
      expect { valid_post.save! }.to_not raise_error
      Post.find(valid_post.permalink).title.should == 'Valid Post'
    end
  end

  describe '#update_attributes!' do
    it 'should raise error and not update data in the database' do
      expect { invalid_post.update_attributes!(:body => 'My Post') }.
        to raise_error(Cequel::Record::RecordInvalid)
    end

    it 'should return successfully and update data in the database if valid' do
      invalid_post.update_attributes!(:title => 'My Post')
      Post.find(invalid_post.permalink).title.should == 'My Post'
    end
  end

  describe '::create!' do
    it 'should raise RecordInvalid and not persist model if invalid' do
      expect do
        Post.create!(:permalink => 'cequel', :body => 'Cequel')
      end.to raise_error(Cequel::Record::RecordInvalid)
    end

    it 'should persist record to database if valid' do
      Post.create!(:permalink => 'cequel', :title => 'Cequel')
      Post.find('cequel').title.should == 'Cequel'
    end
  end

  describe 'callbacks' do
    it 'should call validation callbacks' do
      post = Post.new(:title => 'cequel')
      post.valid?
      post.called_validate_callback.should be_true
    end
  end
end
