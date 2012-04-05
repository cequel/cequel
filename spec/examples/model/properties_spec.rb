require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Properties do
  let(:post) { Post.new }

  it 'should have getter and setter for key' do
    post.id = 1
    post.id.should == 1
  end

  it 'should return key alias from class' do
    Post.key_alias.should == :id
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
      should == [[:id, :integer], [:title, :string]]
  end

  it 'should expose #attributes' do
    post.id = 1
    post.title = 'Cequel'
    post.attributes.
      should == {:id => 1, :title => 'Cequel'}.with_indifferent_access
  end

  it 'should not return nil values with attributes' do
    post.id = 1
    post.title = nil
    post.attributes.should == {:id => 1}.with_indifferent_access
  end

  it 'should set attributes' do
    post.attributes = { :id => 1, :title => 'Cequel' }
    post.id.should == 1
    post.title.should == 'Cequel'
  end

end
