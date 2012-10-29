require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Counter do
  let(:counter) { CommentCounts[1] }
  let(:dictionary) { counter }
  let(:uuid1) { CassandraCQL::UUID.new }
  let(:uuid2) { CassandraCQL::UUID.new }

  it_behaves_like 'readable dictionary'

  describe '#increment' do
    it 'should increment single column by default 1' do
      connection.should_receive(:execute).with(
        'UPDATE comment_counts SET ? = ? + ? WHERE ? = ?',
        uuid1, uuid1, 1, :blog_id, 1
      )
      dictionary.increment(uuid1)
    end

    it 'should increment single column by given value' do
      connection.should_receive(:execute).with(
        'UPDATE comment_counts SET ? = ? + ? WHERE ? = ?',
        uuid1, uuid1, 4, :blog_id, 1
      )
      dictionary.increment(uuid1, 4)
    end

    it 'should increment multiple columns by default value' do
      connection.should_receive(:execute).with(
        'UPDATE comment_counts SET ? = ? + ?, ? = ? + ? WHERE ? = ?',
        uuid1, uuid1, 1, uuid2, uuid2, 1, :blog_id, 1
      )
      dictionary.increment([uuid1, uuid2])
    end

    it 'should increment multiple columns by given value' do
      connection.should_receive(:execute).with(
        'UPDATE comment_counts SET ? = ? + ?, ? = ? + ? WHERE ? = ?',
        uuid1, uuid1, 4, uuid2, uuid2, 4, :blog_id, 1
      )
      dictionary.increment([uuid1, uuid2], 4)
    end

    it 'should increment multiple columns by given deltas' do
      connection.should_receive(:execute).with(
        'UPDATE comment_counts SET ? = ? + ?, ? = ? + ? WHERE ? = ?',
        uuid1, uuid1, 4, uuid2, uuid2, 2, :blog_id, 1
      )
      dictionary.increment(uuid1 => 4, uuid2 => 2)
    end
  end

  describe '#decrement' do
    it 'should increment single column by default 1' do
      connection.should_receive(:execute).with(
        'UPDATE comment_counts SET ? = ? - ? WHERE ? = ?',
        uuid1, uuid1, 1, :blog_id, 1
      )
      dictionary.decrement(uuid1)
    end

    it 'should increment single column by given value' do
      connection.should_receive(:execute).with(
        'UPDATE comment_counts SET ? = ? - ? WHERE ? = ?',
        uuid1, uuid1, 4, :blog_id, 1
      )
      dictionary.decrement(uuid1, 4)
    end

    it 'should increment multiple columns by default value' do
      connection.should_receive(:execute).with(
        'UPDATE comment_counts SET ? = ? - ?, ? = ? - ? WHERE ? = ?',
        uuid1, uuid1, 1, uuid2, uuid2, 1, :blog_id, 1
      )
      dictionary.decrement([uuid1, uuid2])
    end

    it 'should increment multiple columns by given value' do
      connection.should_receive(:execute).with(
        'UPDATE comment_counts SET ? = ? - ?, ? = ? - ? WHERE ? = ?',
        uuid1, uuid1, 4, uuid2, uuid2, 4, :blog_id, 1
      )
      dictionary.decrement([uuid1, uuid2], 4)
    end

    it 'should increment multiple columns by given deltas' do
      connection.should_receive(:execute).with(
        'UPDATE comment_counts SET ? = ? - ?, ? = ? - ? WHERE ? = ?',
        uuid1, uuid1, 4, uuid2, uuid2, 2, :blog_id, 1
      )
      dictionary.decrement(uuid1 => 4, uuid2 => 2)
    end
  end
end
