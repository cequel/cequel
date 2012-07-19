require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Dictionary do
  let(:uuid1) { uuid }
  let(:uuid2) { uuid }
  let(:uuid3) { uuid }
  let(:dictionary) { BlogPosts[1] }

  describe '#save' do
    before do
      connection.stub(:execute).
        with('SELECT FIRST 1000 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
        and_return result_stub(uuid1 => 1, uuid2 => 2)
    end

    it 'should write to row' do
      dictionary[uuid1] = 1
      dictionary[uuid2] = 2
      connection.should_receive(:execute).
        with('UPDATE blog_posts SET ? = ?, ? = ? WHERE ? = ?', uuid1, 1, uuid2, 2, :blog_id, 1)
      dictionary.save
    end

    it 'should update changed columns' do
      dictionary.load
      dictionary[uuid1] = 2
      connection.should_receive(:execute).
        with('UPDATE blog_posts SET ? = ? WHERE ? = ?', uuid1, 2, :blog_id, 1)
      dictionary.save
    end

    it 'should write new columns' do
      dictionary.load
      dictionary[uuid3] = 3
      connection.should_receive(:execute).
        with('UPDATE blog_posts SET ? = ? WHERE ? = ?', uuid3, 3, :blog_id, 1)
      dictionary.save
    end

    it 'should delete removed columns' do
      dictionary.load
      dictionary[uuid1] = nil
      connection.should_receive(:execute).
        with('DELETE ? FROM blog_posts WHERE ? = ?', [uuid1], :blog_id, 1)
      dictionary.save
    end

    it 'should not update a row if it should be deleted' do
      dictionary.load
      dictionary[uuid1] = 1
      dictionary[uuid1] = nil
      connection.should_receive(:execute).once.
        with('DELETE ? FROM blog_posts WHERE ? = ?', [uuid1], :blog_id, 1)
      dictionary.save
    end

    it 'should not delete a row if it was subsequently updated' do
      dictionary.load
      dictionary[uuid1] = nil
      dictionary[uuid1] = 1
      connection.should_receive(:execute).once.
        with('UPDATE blog_posts SET ? = ? WHERE ? = ?', uuid1, 1, :blog_id, 1)
      dictionary.save
    end

    it 'should not save columns that have been saved previously' do
      dictionary.load
      dictionary[uuid3] = 3
      connection.should_receive(:execute).once.
        with('UPDATE blog_posts SET ? = ? WHERE ? = ?', uuid3, 3, :blog_id, 1)
      2.times { dictionary.save }
    end
    
    it 'should not delete columns that have been deleted previously' do
      dictionary.load
      dictionary[uuid1] = nil
      connection.should_receive(:execute).once.
        with('DELETE ? FROM blog_posts WHERE ? = ?', [uuid1], :blog_id, 1)
      2.times { dictionary.save }
    end

  end

  describe '#destroy' do
    before do
      connection.stub(:execute).
        with('SELECT FIRST 1000 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
        and_return result_stub(uuid1 => 1, uuid2 => 2)
      dictionary.load
      connection.should_receive(:execute).
        with('DELETE FROM blog_posts WHERE ? = ?', :blog_id, 1)
    end

    it 'should delete row from cassandra' do
      dictionary.destroy
    end

    it 'should remove all properties from memory' do
      dictionary.destroy
      connection.stub(:execute).
        with('SELECT ? FROM blog_posts WHERE ? = ? LIMIT 1', [uuid1], :blog_id, 1).
        and_return result_stub({})
      dictionary[uuid1].should be_nil
    end

    it 'should not save previously updated properties when saved' do
      dictionary[uuid1] = 5
      dictionary.destroy
      connection.should_not_receive(:execute)
      dictionary.save
    end

    it 'should not delete previously deleted properties when saved' do
      dictionary[uuid1] = nil
      dictionary.destroy
      connection.should_not_receive(:execute)
      dictionary.save
    end
  end

  describe '#each_pair' do
    it 'should iterate over loaded properties' do
      connection.stub(:execute).
        with('SELECT FIRST 1000 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
        and_return result_stub(uuid1 => 1, uuid2 => 2)
      dictionary.load
      hash = {}
      dictionary.each_pair { |column, value| hash[column] = value }
      hash.should == {uuid1 => 1, uuid2 => 2}
    end
  end

  describe '#load_each_pair' do
    it 'should load columns in batches and yield them' do
      connection.should_receive(:execute).
        with('SELECT FIRST 2 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
        and_return result_stub(uuid1 => 1, uuid2 => 2)
      connection.should_receive(:execute).
        with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid2, '', :blog_id, 1).
        and_return result_stub(uuid2 => 2, uuid3 => 3)
      connection.should_receive(:execute).
        with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid3, '', :blog_id, 1).
        and_return result_stub({})
      hash = {}
      dictionary.load_each_pair(:batch_size => 2) do |key, value|
        hash[key] = value
      end
      hash.should == {uuid1 => 1, uuid2 => 2, uuid3 => 3}
    end

  end

  describe '#load' do

    it 'should populate dictionary with all columns' do
      connection.should_receive(:execute).
        with('SELECT FIRST 1000 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
        and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
      dictionary.load
      dictionary[uuid1].should == 1
    end

    it 'should populate dictionary with specified columns' do
      connection.should_receive(:execute).
        with('SELECT ? FROM blog_posts WHERE ? = ? LIMIT 1', [uuid1, uuid2, uuid3], :blog_id, 1).
        and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
      dictionary.load(uuid1, uuid2, uuid3)
    end

    it 'should populate dictionary with column range' do
      connection.should_receive(:execute).
        with('SELECT ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid1, uuid3, :blog_id, 1).
        and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
      dictionary.load(uuid1..uuid3)
    end

    it 'should populate dictionary with greater-than range' do
      connection.should_receive(:execute).
        with('SELECT ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid1, '', :blog_id, 1).
        and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
      dictionary.load(:from => uuid1)
    end

  end

  describe '#[]' do
    it 'should load column from cassandra if it has not already' do
      connection.stub(:execute).
        with('SELECT ? FROM blog_posts WHERE ? = ? LIMIT 1', [uuid1], :blog_id, 1).
        and_return result_stub(uuid1 => 1)
      dictionary[uuid1].should == 1
    end

    it 'should not reload column if it has been loaded' do
      connection.stub(:execute).
        with('SELECT ? FROM blog_posts WHERE ? = ? LIMIT 1', [uuid1], :blog_id, 1).
        and_return result_stub(uuid1 => 1)
      dictionary.load(uuid1)
      connection.should_not_receive(:execute)
      dictionary[uuid1].should == 1
    end

    it 'should not reload column if it was not found by previous load' do
      connection.stub(:execute).
        with('SELECT ? FROM blog_posts WHERE ? = ? LIMIT 1', [uuid1], :blog_id, 1).
        and_return result_stub({})
      dictionary.load(uuid1)
      connection.should_not_receive(:execute)
      dictionary[uuid1].should be_nil
    end

    it 'should not reload missing column if all columns already loaded' do
      connection.stub(:execute).
        with('SELECT FIRST 1000 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
        and_return result_stub(uuid2 => 2)
      dictionary.load
      dictionary[uuid1].should be_nil
    end
  end

  private

  def uuid
    SimpleUUID::UUID.new.to_guid
  end
end
