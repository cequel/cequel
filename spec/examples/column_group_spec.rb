require File.expand_path('../spec_helper', __FILE__)

describe Cequel::ColumnGroup do
  describe '#insert' do
    it 'should insert a row' do
      connection.should_receive(:execute).
        with 'INSERT INTO posts (id, title) VALUES (?, ?)', 1, 'Fun times'
      cequel[:posts].insert(:id => 1, :title => 'Fun times')
    end
  end
end
