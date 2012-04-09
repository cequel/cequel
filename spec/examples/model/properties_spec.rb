require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Properties do
  let(:post) { Post.new(1) }

  it 'should have getter for key' do
    post.id.should == 1
  end

  it 'should not have setter for key' do
    expect { post.id = 2 }.to raise_error(NoMethodError)
  end

  it 'should return key alias from class' do
    Post.key_alias.should == :id
  end

  it 'should return key for to_key' do
    post.to_key.should == [1]
  end

  it 'should return param for to_param' do
    post.persisted!
    post.to_param.should == '1'
  end

  it 'should have getter and setter for column' do
    post.title = 'Object/row mapping'
    post.title.should == 'Object/row mapping'
  end

  it 'should expose column names on class' do
    Post.column_names[0..1].should == [:id, :title]
  end

  it 'should expose column objects on class' do
    Post.columns[0..1].map { |col| [col.name, col.type] }.
      should == [[:id, :integer], [:title, :varchar]]
  end

  it 'should expose #attributes' do
    post.title = 'Cequel'
    post.attributes.
      should == {:id => 1, :title => 'Cequel'}.with_indifferent_access
  end

  it 'should not return nil values with attributes' do
    post.title = nil
    post.attributes.should == {:id => 1}.with_indifferent_access
  end

  it 'should set attributes' do
    post.attributes = { :title => 'Cequel' }
    post.id.should == 1
    post.title.should == 'Cequel'
  end

  it 'should set attributes from constructor' do
    Post.new(1, :title => 'Cequel').title.should == 'Cequel'
  end

end
