require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Inheritable do
  it 'should inherit key from superclass' do
    Photo.key_alias.should == :id
  end

  it 'should inherit columns from superclass' do
    Photo.column_names.should == [:id, :class_name, :label, :checksum, :url]
  end

  it 'should return ::base_class for base class' do
    Asset.base_class.should == Asset
  end

  it 'should return ::base_class for subclass' do
    Photo.base_class.should == Asset
  end

  it 'should not allow overriding a class without a type column' do
    base = Class.new do
      include Cequel::Model
      key :id, :integer
    end
    expect { sub = Class.new(base) }.to raise_error(ArgumentError)
  end

  it 'should set type on initialization' do
    Photo.new.class_name.should == 'Photo'
  end

  it 'should query correct column family when querying subclass' do
    connection.stub(:execute).
      with("SELECT * FROM assets WHERE id = 1 LIMIT 1").
      and_return result_stub(:id => 1, :label => 'Cequel')
    Photo.find(1)
  end

  it 'should hydrate correct model class when querying base class' do
    connection.stub(:execute).
      with('SELECT * FROM assets WHERE id = 1 LIMIT 1').
      and_return result_stub(:id => 1, :class_name => 'Photo', :label => 'Cequel')
    Asset.find(1).should be_a(Photo)
  end
end
