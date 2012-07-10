require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Dynamic do

  let(:category) { Category.new(:id => 1, :name => 'Big Data') }

  it 'should allow getting and setting of dynamic attributes' do
    category[:tag] = 'bigdata'
    category[:tag].should == 'bigdata'
  end

  it 'should insert dynamic values' do
    category[:tag] = 'bigdata'
    connection.should_receive(:execute).
      with "INSERT INTO categories (id, name, tag) VALUES (?, ?, ?)", 1, 'Big Data', 'bigdata'
    category.save
  end

  it 'should update dynamic values' do
    category[:tag] = 'bigdata'
    connection.stub(:execute).
      with "INSERT INTO categories (id, name, tag) VALUES (?, ?, ?)", 1, 'Big Data', 'bigdata'
    category.save
    category[:tag] = 'big-data'
    category[:color] = 'blue'
    connection.should_receive(:execute).
      with "UPDATE categories SET tag = ?, color = ? WHERE id = ?", 'big-data', 'blue', 1
    category.save
  end

  it 'should delete dynamic values' do
    category[:tag] = 'bigdata'
    connection.stub(:execute).
      with "INSERT INTO categories (id, name, tag) VALUES (?, ?, ?)", 1, 'Big Data', 'bigdata'
    category.save
    category[:tag] = nil
    connection.should_receive(:execute).
      with "DELETE tag FROM categories WHERE id = ?", 1
    category.save
  end
end
