require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Dictionary do
  let(:uuid1) { uuid }
  let(:uuid2) { uuid }
  let(:uuid3) { uuid }
  let(:dictionary) { BlogPosts[1] }

  describe '#save' do
    before do
      connection.stub(:execute).
        with('SELECT FIRST 2 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
        and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
      connection.stub(:execute).
        with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid2, '', :blog_id, 1).
        and_return result_stub('blog_id' => 1)
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
        with('SELECT FIRST 2 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
        and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
      connection.stub(:execute).
        with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid2, '', :blog_id, 1).
        and_return result_stub({'blog_id' => 1})

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

  context 'without row in memory' do

    describe '#each_pair' do

      it 'should load columns in batches and yield them' do
        connection.should_receive(:execute).
          with('SELECT FIRST 2 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
        connection.should_receive(:execute).
          with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid2, '', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid2 => 2, uuid3 => 3)
        connection.should_receive(:execute).
          with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid3, '', :blog_id, 1).
          and_return result_stub({'blog_id' => 1})
        hash = {}
        dictionary.each_pair do |key, value|
          hash[key] = value
        end
        hash.should == {uuid1 => 1, uuid2 => 2, uuid3 => 3}
      end

    end

    describe '#[]' do

      it 'should load column from cassandra' do
        connection.stub(:execute).
          with('SELECT ? FROM blog_posts WHERE ? = ? LIMIT 1', [uuid1], :blog_id, 1).
          and_return result_stub(uuid1 => 1)
        dictionary[uuid1].should == 1
      end

    end

    describe '#slice' do
      it 'should load columns from data store' do
        connection.stub(:execute).
          with('SELECT ? FROM blog_posts WHERE ? = ? LIMIT 1', [uuid1,uuid2], :blog_id, 1).
          and_return result_stub(uuid1 => 1, uuid2 => 2)
        dictionary.slice(uuid1, uuid2).should == {uuid1 => 1, uuid2 => 2}
      end
    end

    describe '#keys' do
      it 'should load keys from data store' do
        connection.should_receive(:execute).
          with('SELECT FIRST 2 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
        connection.should_receive(:execute).
          with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid2, '', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid2 => 2, uuid3 => 3)
        connection.should_receive(:execute).
          with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid3, '', :blog_id, 1).
          and_return result_stub({'blog_id' => 1})
        dictionary.keys.should == [uuid1, uuid2, uuid3]
      end
    end

    describe '#values' do
      it 'should load values from data store' do
        connection.should_receive(:execute).
          with('SELECT FIRST 2 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
        connection.should_receive(:execute).
          with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid2, '', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid2 => 2, uuid3 => 3)
        connection.should_receive(:execute).
          with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid3, '', :blog_id, 1).
          and_return result_stub({'blog_id' => 1})
        dictionary.values.should == [1, 2, 3]
      end
    end

  end

  context 'with data loaded in memory' do
    before do
      connection.stub(:execute).
        with('SELECT FIRST 2 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
        and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
      connection.stub(:execute).
        with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid2, '', :blog_id, 1).
        and_return result_stub('blog_id' => 1, uuid2 => 2, uuid3 => 3)
      connection.stub(:execute).
        with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid3, '', :blog_id, 1).
        and_return result_stub({'blog_id' => 1})
      dictionary.load
      connection.should_not_receive(:execute)
    end

    describe '#each_pair' do
      it 'should yield data from memory' do
        hash = {}
        dictionary.each_pair do |key, value|
          hash[key] = value
        end
        hash.should == {uuid1 => 1, uuid2 => 2, uuid3 => 3}
      end
    end

    describe '#[]' do
      it 'should return value from memory' do
        dictionary[uuid1].should == 1
      end
    end

    describe '#slice' do
      it 'should return slice of data in memory' do
        dictionary.slice(uuid1, uuid2).should == {uuid1 => 1, uuid2 => 2}
      end
    end

    describe '#keys' do
      it 'should return keys from memory' do
        dictionary.keys.should == [uuid1, uuid2, uuid3]
      end
    end

    describe '#values' do
      it 'should return values from memory' do
        dictionary.values.should == [1, 2, 3]
      end
    end
  end

  context 'with data modified but not loaded in memory' do
    let(:uuid4) { uuid }

    before do
      dictionary[uuid1] = -1
      dictionary[uuid3] = nil
      dictionary[uuid4] = 4
    end

    describe '#each_pair' do
      it 'should override persisted data with unsaved changes' do
        connection.stub(:execute).
          with('SELECT FIRST 2 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
        connection.stub(:execute).
          with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid2, '', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid2 => 2, uuid3 => 3)
        connection.stub(:execute).
          with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid3, '', :blog_id, 1).
          and_return result_stub({'blog_id' => 1})
        hash = {}
        dictionary.each_pair do |key, value|
          hash[key] = value
        end
        hash.should == {uuid1 => -1, uuid2 => 2, uuid4 => 4}
      end
    end

    describe '#[]' do
      it 'should return unsaved changed value' do
        dictionary[uuid1].should == -1
      end

      it 'should return nil if value removed' do
        dictionary[uuid3].should be_nil
      end

      it 'should return unsaved new value' do
        dictionary[uuid4].should == 4
      end
    end

    describe '#slice' do
      it 'should override loaded slice with unsaved data in memory' do
        connection.stub(:execute).
          with('SELECT ? FROM blog_posts WHERE ? = ? LIMIT 1', [uuid1,uuid2,uuid3,uuid4], :blog_id, 1).
          and_return result_stub(uuid1 => 1, uuid2 => 2, uuid3 => 3)
        dictionary.slice(uuid1, uuid2, uuid3, uuid4).should ==
          {uuid1 => -1, uuid2 => 2, uuid4 => 4}
      end
    end

    describe '#keys' do
      it 'should override keys that have been added or removed' do
        connection.should_receive(:execute).
          with('SELECT FIRST 2 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
        connection.should_receive(:execute).
          with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid2, '', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid2 => 2, uuid3 => 3)
        connection.should_receive(:execute).
          with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid3, '', :blog_id, 1).
          and_return result_stub({'blog_id' => 1})
        dictionary.keys.should == [uuid1, uuid2, uuid4]
      end
    end

    describe '#values' do
      it 'should override values that have been added or removed' do
        connection.should_receive(:execute).
          with('SELECT FIRST 2 * FROM blog_posts WHERE ? = ? LIMIT 1', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
        connection.should_receive(:execute).
          with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid2, '', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid2 => 2, uuid3 => 3)
        connection.should_receive(:execute).
          with('SELECT FIRST 2 ?..? FROM blog_posts WHERE ? = ? LIMIT 1', uuid3, '', :blog_id, 1).
          and_return result_stub({'blog_id' => 1})
        dictionary.values.should == [-1, 2, 4]
      end
    end
  end

  context 'with serializer/deserializer defined' do
    let(:dictionary) { PostComments[1] }
    let(:comment) { {'user' => 'Mat Brown', 'comment' => 'I like big data.'} }

    describe '#save' do
      it 'should serialize data' do
        dictionary[4] = comment
        connection.should_receive(:execute).
          with(
            'UPDATE post_comments SET ? = ? WHERE ? = ?',
            4, comment.to_json, :post_id, 1
          )
        dictionary.save
      end
    end

    describe '#[]' do
      it 'should return deserialized data' do
        connection.stub(:execute).with(
            'SELECT ? FROM post_comments WHERE ? = ? LIMIT 1',
            [4], :post_id, 1
        ).and_return result_stub(4 => comment.to_json)
        dictionary[4].should == comment
      end
    end

    describe '#slice' do
      it 'should return deserialized values' do
        connection.stub(:execute).with(
          'SELECT ? FROM post_comments WHERE ? = ? LIMIT 1',
          [4, 5], :post_id, 1
        ).and_return result_stub(4 => comment.to_json)
        dictionary.slice(4, 5).should == {4 => comment}
      end
    end

    describe '#load' do
      it 'should retain deserialized values in memory' do
        connection.stub(:execute).with(
          'SELECT FIRST 1000 * FROM post_comments WHERE ? = ? LIMIT 1',
          :post_id, 1
        ).and_return result_stub(4 => comment.to_json)
        dictionary.load
        connection.should_not_receive(:execute)
        dictionary[4].should == comment
      end
    end

    describe '#each_pair' do
      it 'should yield deserialized values' do
        connection.stub(:execute).
          with(
            'SELECT FIRST 1000 * FROM post_comments WHERE ? = ? LIMIT 1',
            :post_id, 1
          ).and_return result_stub('post_id' => 1, 4 => comment.to_json)
        dictionary.each_pair.map { |column, comment| comment }.first.
          should == comment
      end
    end

    describe '#values' do
      it 'should return deserialized values' do
        connection.stub(:execute).
          with(
            'SELECT FIRST 1000 * FROM post_comments WHERE ? = ? LIMIT 1',
            :post_id, 1
          ).and_return result_stub('post_id' => 1, 4 => comment.to_json)
        dictionary.values.should == [comment]
      end
    end
  end

  private

  def uuid
    SimpleUUID::UUID.new.to_guid
  end
end
