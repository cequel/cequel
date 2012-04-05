require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Properties do
  it 'should have getter and setter for key' do
    post = Post.new
    post.id = 1
    post.id.should == 1
  end

  it 'should return key alias from class' do
    Post.key_alias.should == :id
  end
end
