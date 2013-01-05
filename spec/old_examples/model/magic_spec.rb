require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Magic do
  describe '::find_by_*' do
    it 'should magically look up one record by given value' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ? LIMIT 1", :title, 'Cequel').
        and_return result_stub(:id => 1, :title => 'Cequel')

      Post.find_by_title('Cequel').id.should == 1
    end

    it 'should magically look up one record by multiple values' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ? AND ? = ? LIMIT 1", :title, 'Cequel', :published, true).
        and_return result_stub(:id => 1, :title => 'Cequel', :published => true)

      Post.find_by_title_and_published('Cequel', true).id.should == 1
    end

    it 'should magically work on scopes' do
      connection.stub(:execute).
        with("SELECT ? FROM posts WHERE ? = ? AND ? = ? LIMIT 1", [:id], :title, 'Cequel', :published, true).
        and_return result_stub(:id => 1)

      Post.select(:id).find_by_title_and_published('Cequel', true).id.should == 1
    end

    it 'should raise error if specified columns different from arg count' do
      expect { Post.find_by_title_and_published('Cequel') }.
        to raise_error(ArgumentError)
    end
  end

  describe '::find_all_by_*' do
    context 'with existing record specified as args' do
      it 'should magically look up one record by multiple values' do
        connection.stub(:execute).
          with("SELECT * FROM posts WHERE ? = ? AND ? = ?", :title, 'Cequel', :published, true).
          and_return result_stub(
            {:id => 1, :title => 'Cequel', :published => true},
            {:id => 2, :title => 'Cequel2', :published => true }
          )

        Post.find_all_by_title_and_published('Cequel', true).map(&:id).
          should == [1, 2]
      end

      it 'should magically work on scopes' do
        connection.stub(:execute).
          with("SELECT ? FROM posts WHERE ? = ? AND ? = ?", [:id], :title, 'Cequel', :published, true).
          and_return result_stub(
            {:id => 1},
            {:id => 2}
          )

        Post.select(:id).find_all_by_title_and_published('Cequel', true).map(&:id).
          should == [1, 2]
      end
    end
  end

  describe '::find_or_create_by_*' do
    it 'should return existing record from args' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ? AND ? = ? LIMIT 1", :title, 'Cequel', :published, true).
        and_return result_stub(:id => 1, :title => 'Cequel', :published => true)

      Post.find_or_create_by_title_and_published('Cequel', true).id.should == 1
    end

    it 'should create new record from args' do
      now = Time.now
      Time.stub!(:now).and_return now
      connection.stub(:execute).
        with("SELECT * FROM blogs WHERE ? = ? LIMIT 1", :id, 1).
        and_return result_stub
      connection.should_receive(:execute).
        with(
          "INSERT INTO blogs (?) VALUES (?)",
          ['id', 'published', 'updated_at', 'created_at'],
          [1, true, now, now]
        )

      Blog.find_or_create_by_id(1).id.should == 1
    end

    it 'should look up record from attributes' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ? AND ? = ? LIMIT 1", :title, 'Cequel', :published, true).
        and_return result_stub(:id => 1, :title => 'Cequel', :published => true)

      Post.find_or_create_by_title_and_published(
        :id => 2, :title => 'Cequel', :published => true
      ).id.should == 1
    end

    it 'should create record from all attributes specified, including non-lookup ones' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ? AND ? = ? LIMIT 1", :title, 'Cequel', :published, true).
        and_return result_stub
      connection.should_receive(:execute).
        with(
          "INSERT INTO posts (?) VALUES (?)",
          ['id', 'title', 'published'],
          [2, 'Cequel', true])

      Post.find_or_create_by_title_and_published(
        :id => 2, :title => 'Cequel', :published => true
      ).id.should == 2
    end

    it 'should yield instance on create if block given' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ? AND ? = ? LIMIT 1", :title, 'Cequel', :published, true).
        and_return result_stub
      connection.should_receive(:execute).
        with(
          "INSERT INTO posts (?) VALUES (?)",
          ['id', 'title', 'published'],
          [2, 'Cequel', true]
        )

      Post.find_or_create_by_title_and_published('Cequel', true) do |post|
        post.id = 2
      end.id.should == 2
    end

    it 'should work on scopes' do
      connection.stub(:execute).
        with("SELECT ? FROM posts WHERE ? = ? AND ? = ? LIMIT 1", [:id], :title, 'Cequel', :published, true).
        and_return result_stub(:id => 1)

      Post.select(:id).find_or_create_by_title_and_published('Cequel', true).id.
        should == 1
    end
  end

  describe '::find_or_initialize_by_*' do
    it 'should return existing record from args' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ? AND ? = ? LIMIT 1", :title, 'Cequel', :published, true).
        and_return result_stub(:id => 1, :title => 'Cequel', :published => true)

      Post.find_or_initialize_by_title_and_published('Cequel', true).id.should == 1
    end

    it 'should initialize new record from args' do
      now = Time.now
      Time.stub!(:now).and_return now
      timestamp = (now.to_f * 1000).to_i
      connection.stub(:execute).
        with("SELECT * FROM blogs WHERE ? = ? LIMIT 1", :id, 1).
        and_return result_stub

      Blog.find_or_initialize_by_id(1).id.should == 1
    end

    it 'should look up record from attributes' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ? AND ? = ? LIMIT 1", :title, 'Cequel', :published, true).
        and_return result_stub(:id => 1, :title => 'Cequel', :published => true)

      Post.find_or_initialize_by_title_and_published(
        :id => 2, :title => 'Cequel', :published => true
      ).id.should == 1
    end

    it 'should create record from all attributes specified, including non-lookup ones' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ? AND ? = ? LIMIT 1", :title, 'Cequel', :published, true).
        and_return result_stub

      Post.find_or_initialize_by_title_and_published(
        :id => 2, :title => 'Cequel', :published => true
      ).id.should == 2
    end

    it 'should yield instance on initialize if block given' do
      connection.stub(:execute).
        with("SELECT * FROM posts WHERE ? = ? AND ? = ? LIMIT 1", :title, 'Cequel', :published, true).
        and_return result_stub

      Post.find_or_initialize_by_title_and_published('Cequel', true) do |post|
        post.id = 2
      end.id.should == 2
    end

    it 'should work on scopes' do
      connection.stub(:execute).
        with("SELECT ? FROM posts WHERE ? = ? AND ? = ? LIMIT 1", [:id], :title, 'Cequel', :published, true).
        and_return result_stub(:id => 1)

      Post.select(:id).find_or_initialize_by_title_and_published('Cequel', true).id.
        should == 1
    end

  end
end
