require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Validations do
  describe '#valid?' do
    it 'should be false if model is not valid' do
      Post.new(:id => 1, :require_title => true).should_not be_valid
    end

    it 'should be true if model is valid' do
      Post.new(:id => 1, :require_title => true, :title => 'Cequel').should be_valid
    end
  end

  describe '#save' do
    it 'should return false and not persist model if invalid' do
      Post.new(:id => 1, :body => 'Cequel', :require_title => true).save.should be_false
    end

    it 'should return true and persist model if valid' do
      connection.should_receive(:execute).
        with "INSERT INTO posts (id, title) VALUES (1, 'Cequel')"

      Post.new(:id => 1, :title => 'Cequel', :require_title => true).save.should be_true
    end
  end

  describe '#save!' do
    it 'should raise error and not persist model if invalid' do
      expect do
        Post.new(:id => 1, :body => 'Cequel', :require_title => true).save!
      end.to raise_error(Cequel::Model::RecordInvalid)
    end

    it 'should persist model and return self if valid' do
      connection.should_receive(:execute).
        with "INSERT INTO posts (id, title) VALUES (1, 'Cequel')"

      post = Post.new(:id => 1, :title => 'Cequel', :require_title => true)
      post.save!.should == post
    end
  end

  describe '#update_attributes!' do
    let(:post) do
      connection.stub(:execute).with("SELECT * FROM posts WHERE id = 1 LIMIT 1").
        and_return result_stub(:id => 1, :blog_id => 1, :title => 'Cequel')
      Post.find(1)
    end

    it 'should change attributes and save them if valid' do
      connection.should_receive(:execute).
        with "UPDATE posts SET body = 'Cequel cequel' WHERE id = 1"
      post.update_attributes!(:body => 'Cequel cequel')
    end

    it 'should raise error if not valid' do
      post.require_title = true
      expect { post.update_attributes!(:title => nil) }.
        to raise_error(Cequel::Model::RecordInvalid)
    end
  end

  describe '::create!' do
    it 'should raise RecordInvalid and not persist model if invalid' do
      expect do
        Post.create!(:id => 1, :body => 'Cequel', :require_title => true)
      end.to raise_error(Cequel::Model::RecordInvalid)
    end

    it 'should and return model if valid' do
      connection.should_receive(:execute).
        with "INSERT INTO posts (id, title) VALUES (1, 'Cequel')"

      Post.create!(:id => 1, :title => 'Cequel', :require_title => true).
        title.should == 'Cequel'
    end
  end

  describe 'callbacks' do
    it 'should call validation callbacks' do
      post = Post.new(:id => 1)
      post.valid?
      post.should have_callback(:validation)
    end
  end
end
