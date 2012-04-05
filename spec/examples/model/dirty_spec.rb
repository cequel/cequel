require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Dirty do
  let(:post) { Post.new(1) }

  it 'should not have changed attributes by default' do
    post.changed_attributes.should be_empty
  end

  it 'should have changed attributes if attributes change' do
    post.title = 'Cequel'
    post.changed_attributes.should == {:title => nil}
  end

  it 'should not have changed attributes if attribute set to the same thing' do
    post.title = nil
    post.changed_attributes.should be_empty
  end

  it 'should not have changed attributes after save' do
    connection.stub(:execute)
    post.title = 'Cequel'
    post.save
    post.changed_attributes.should be_empty
  end

  it 'should have previous changes after save' do
    connection.stub(:execute)
    post.title = 'Cequel'
    post.save
    post.previous_changes.should == { :title => [nil, 'Cequel'] }
  end
end
