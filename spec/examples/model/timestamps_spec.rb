require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Timestamps do
  let!(:now) { Time.now }
  before { Time.stub(:now).and_return now }

  it 'should add created_at column' do
    Blog.column_names.should include(:created_at)
  end

  it 'should add updated_at column' do
    Blog.column_names.should include(:updated_at)
  end

  it 'should populate created_at and updated_at column on create' do
    connection.should_receive(:execute).
      with "INSERT INTO blogs (?) VALUES (?)",
        ['id', 'published', 'updated_at', 'created_at'],
        [1, true, now, now]
    Blog.create!(:id => 1)
  end

  it 'should populate updated_at column on update' do
    connection.stub(:execute).
      with("SELECT * FROM blogs WHERE ? = ? LIMIT 1", :id, 1).
      and_return result_stub(
        :id => 1,
        :name => 'Blogtime',
        :created_at => now - 1.day,
        :updated_at => now - 1.day
      )
    blog = Blog.find(1)
    blog.name = 'Bloggy'
    connection.should_receive(:execute).
      with("UPDATE blogs SET ? = ?, ? = ? WHERE ? = ?", 'name', 'Bloggy', 'updated_at', now, :id, 1)
    blog.save
  end

  it 'should not update updated_at when calling save with no dirty attributes' do
    connection.stub(:execute).
      with("SELECT * FROM blogs WHERE ? = ? LIMIT 1", :id, 1).
      and_return result_stub(
        :id => 1,
        :name => 'Blogtime',
        :created_at => now - 1.day,
        :updated_at => now - 1.day
      )
    blog = Blog.find(1)
    blog.save
    blog.updated_at.should == now - 1.day
  end
end
