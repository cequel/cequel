require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Callbacks do
  let(:post) do
    connection.stub(:execute).
      with("SELECT * FROM posts WHERE id = ? LIMIT 1", 1).
      and_return result_stub(:id => 1, :title => 'Cequel')
    Post.find(1)
  end

  context 'on create' do
    let(:post) { Post.new(:id => 1) }

    before do
      post.save
    end

    it 'should invoke save callback' do
      post.should have_callback(:save)
    end

    it 'should invoke create callback' do
      post.should have_callback(:create)
    end

    it 'should not invoke update callback' do
      post.should_not have_callback(:update)
    end

    it 'should not invoke destroy callback' do
      post.should_not have_callback(:destroy)
    end
  end

  context 'on update' do
    before do
      post.save
    end

    it 'should invoke save callback' do
      post.should have_callback(:save)
    end

    it 'should not invoke create callback' do
      post.should_not have_callback(:create)
    end

    it 'should invoke update callback' do
      post.should have_callback(:update)
    end

    it 'should not invoke destroy callback' do
      post.should_not have_callback(:destroy)
    end
  end

  context 'on destroy' do
    before do
      connection.stub(:execute).with("DELETE FROM posts WHERE id = ?", 1)
      post.destroy
    end

    it 'should not invoke save callback' do
      post.should_not have_callback(:save)
    end

    it 'should not invoke create callback' do
      post.should_not have_callback(:create)
    end

    it 'should not invoke update callback' do
      post.should_not have_callback(:update)
    end

    it 'should invoke destroy callback' do
      post.should have_callback(:destroy)
    end
  end
end
