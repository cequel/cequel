# -*- encoding : utf-8 -*-
shared_examples 'readable dictionary' do
  let(:uuid1) { uuid }
  let(:uuid2) { uuid }
  let(:uuid3) { uuid }
  let(:cf) { dictionary.class.column_family.column_family }

  context 'without row in memory' do

    describe '#each_pair' do

      it 'should load columns in batches and yield them' do
        expect(connection).to receive(:execute).
          with("SELECT FIRST 2 * FROM #{cf} WHERE ? = ? LIMIT 1", :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
        expect(connection).to receive(:execute).
          with("SELECT FIRST 2 ?..? FROM #{cf} WHERE ? = ? LIMIT 1", uuid2, '', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid2 => 2, uuid3 => 3)
        expect(connection).to receive(:execute).
          with("SELECT FIRST 2 ?..? FROM #{cf} WHERE ? = ? LIMIT 1", uuid3, '', :blog_id, 1).
          and_return result_stub({'blog_id' => 1})
        hash = {}
        dictionary.each_pair do |key, value|
          hash[key] = value
        end
        expect(hash).to eq({uuid1 => 1, uuid2 => 2, uuid3 => 3})
      end

    end

    describe '#[]' do

      it 'should load column from cassandra' do
        allow(connection).to receive(:execute).
          with("SELECT ? FROM #{cf} WHERE ? = ? LIMIT 1", [uuid1], :blog_id, 1).
          and_return result_stub(uuid1 => 1)
        expect(dictionary[uuid1]).to eq(1)
      end

    end

    describe '#slice' do
      it 'should load columns from data store' do
        allow(connection).to receive(:execute).
          with("SELECT ? FROM #{cf} WHERE ? = ? LIMIT 1", [uuid1,uuid2], :blog_id, 1).
          and_return result_stub(uuid1 => 1, uuid2 => 2)
        expect(dictionary.slice(uuid1, uuid2)).to eq({uuid1 => 1, uuid2 => 2})
      end
    end

    describe '#keys' do
      it 'should load keys from data store' do
        expect(connection).to receive(:execute).
          with("SELECT FIRST 2 * FROM #{cf} WHERE ? = ? LIMIT 1", :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
        expect(connection).to receive(:execute).
          with("SELECT FIRST 2 ?..? FROM #{cf} WHERE ? = ? LIMIT 1", uuid2, '', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid2 => 2, uuid3 => 3)
        expect(connection).to receive(:execute).
          with("SELECT FIRST 2 ?..? FROM #{cf} WHERE ? = ? LIMIT 1", uuid3, '', :blog_id, 1).
          and_return result_stub({'blog_id' => 1})
        expect(dictionary.keys).to eq([uuid1, uuid2, uuid3])
      end
    end

    describe '#values' do
      it 'should load values from data store' do
        expect(connection).to receive(:execute).
          with("SELECT FIRST 2 * FROM #{cf} WHERE ? = ? LIMIT 1", :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
        expect(connection).to receive(:execute).
          with("SELECT FIRST 2 ?..? FROM #{cf} WHERE ? = ? LIMIT 1", uuid2, '', :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid2 => 2, uuid3 => 3)
        expect(connection).to receive(:execute).
          with("SELECT FIRST 2 ?..? FROM #{cf} WHERE ? = ? LIMIT 1", uuid3, '', :blog_id, 1).
          and_return result_stub({'blog_id' => 1})
        expect(dictionary.values).to eq([1, 2, 3])
      end
    end

    describe '#first' do
      it 'should load value from data store' do
        expect(connection).to receive(:execute).
          with("SELECT FIRST 1 * FROM #{cf} WHERE ? = ? LIMIT 1", :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid1 => 1)
        expect(dictionary.first).to eq([uuid1, 1])
      end
    end

    describe '#last' do
      it 'should load value from data store' do
        expect(connection).to receive(:execute).
          with("SELECT FIRST 1 REVERSED * FROM #{cf} WHERE ? = ? LIMIT 1", :blog_id, 1).
          and_return result_stub('blog_id' => 1, uuid3 => 3)
        expect(dictionary.last).to eq([uuid3, 3])
      end
    end

  end

  context 'with data loaded in memory' do
    before do
      allow(connection).to receive(:execute).
        with("SELECT FIRST 2 * FROM #{cf} WHERE ? = ? LIMIT 1", :blog_id, 1).
        and_return result_stub('blog_id' => 1, uuid1 => 1, uuid2 => 2)
      allow(connection).to receive(:execute).
        with("SELECT FIRST 2 ?..? FROM #{cf} WHERE ? = ? LIMIT 1", uuid2, '', :blog_id, 1).
        and_return result_stub('blog_id' => 1, uuid2 => 2, uuid3 => 3)
      allow(connection).to receive(:execute).
        with("SELECT FIRST 2 ?..? FROM #{cf} WHERE ? = ? LIMIT 1", uuid3, '', :blog_id, 1).
        and_return result_stub({'blog_id' => 1})
      dictionary.load
      expect(connection).not_to receive(:execute)
    end

    describe '#each_pair' do
      it 'should yield data from memory' do
        hash = {}
        dictionary.each_pair do |key, value|
          hash[key] = value
        end
        expect(hash).to eq({uuid1 => 1, uuid2 => 2, uuid3 => 3})
      end
    end

    describe '#[]' do
      it 'should return value from memory' do
        expect(dictionary[uuid1]).to eq(1)
      end
    end

    describe '#slice' do
      it 'should return slice of data in memory' do
        expect(dictionary.slice(uuid1, uuid2)).to eq({uuid1 => 1, uuid2 => 2})
      end
    end

    describe '#first' do
      it 'should return first element in memory' do
        expect(dictionary.first).to eq([uuid1, 1])
      end
    end

    describe '#last' do
      it 'should return first element in memory' do
        expect(dictionary.last).to eq([uuid3, 3])
      end
    end

    describe '#keys' do
      it 'should return keys from memory' do
        expect(dictionary.keys).to eq([uuid1, uuid2, uuid3])
      end
    end

    describe '#values' do
      it 'should return values from memory' do
        expect(dictionary.values).to eq([1, 2, 3])
      end
    end
  end

  describe '::load' do
    let :comments do
      [
        {'user' => 'Cequel User', 'comment' => 'How do I load multiple rows?'},
        {'user' => 'Mat Brown', 'comment' => 'Just use the ::load class method'}
      ]
    end

    it 'should load all rows in one query' do
      allow(connection).to receive(:execute).
        with(
          'SELECT * FROM post_comments WHERE ? IN (?)',
          'post_id', [1, 2]
        ).and_return result_stub(
          *comments.each_with_index.
            map { |comment, i| {'post_id' => i+1, i+4 => comment.to_json} }
          )
      rows = PostComments.load(1, 2)
      expect(rows.map { |row| row.post_id }).to eq([1, 2])
      expect(rows.map { |row| row.values.first }).to eq(comments)
    end
  end

  private

  def uuid
    SimpleUUID::UUID.new.to_guid
  end
end

