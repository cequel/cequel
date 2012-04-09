require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::MassAssignmentSecurity do
  let(:post) { Post.new(1, :title => 'Cequel', :blog_id => 3) }

  it 'should allow setting of unprotected attributes' do
    post.title.should == 'Cequel'
  end

  it 'should not allow setting of protected attributes' do
    post.blog_id.should be_nil
  end
end
