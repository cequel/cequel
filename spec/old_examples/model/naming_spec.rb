require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Naming do

  it 'should give correct model_name' do
    Post.model_name.should == 'Post'
  end

end
