require File.expand_path('../../spec_helper', __FILE__)

describe Cequel::Metal::DataSet do
  before :all do
    cequel.schema.create_table(:posts) do
      key :blog_subdomain, :text
      key :permalink, :text
      column :title, :text
      column :body, :text
      column :published_at, :timestamp
      list :categories, :text
      set :tags, :text
      map :trackbacks, :timestamp, :text
    end
    cequel.schema.create_table :post_activity do
      key :blog_subdomain, :text
      key :permalink, :text
      column :visits, :counter
      column :tweets, :counter
    end
  end

  after :each do
    subdomains = cequel[:posts].select(:blog_subdomain).
      map { |row| row[:blog_subdomain] }
    cequel[:posts].where(blog_subdomain: subdomains).delete if subdomains.any?
  end

  after :all do
    cequel.schema.drop_table(:posts)
    cequel.schema.drop_table(:post_activity)
  end

  let(:row_keys) { {blog_subdomain: 'cassandra', permalink: 'big-data'} }

  describe '#insert' do
    let(:row) do
      row_keys.merge(
        title: 'Fun times',
        categories: ['Fun', 'Profit'],
        tags: Set['cassandra', 'big-data'],
        trackbacks: {
          Time.at(Time.now.to_i) => 'www.google.com',
          Time.at(Time.now.to_i - 60) => 'www.yahoo.com'
        }
      )
    end

    it 'should insert a row' do
      cequel[:posts].insert(row)
      cequel[:posts].where(row_keys).first[:title].should == 'Fun times'
    end

    it 'should correctly insert a list' do
      cequel[:posts].insert(row)
      cequel[:posts].where(row_keys).first[:categories].
        should == ['Fun', 'Profit']
    end

    it 'should correctly insert a set' do
      cequel[:posts].insert(row)
      cequel[:posts].where(row_keys).first[:tags].
        should == Set['cassandra', 'big-data']
    end

    it 'should correctly insert a map' do
      cequel[:posts].insert(row)
      cequel[:posts].where(row_keys).first[:trackbacks].
        should == row[:trackbacks]
    end

    it 'should include ttl argument' do
      cequel[:posts].insert(row, :ttl => 10.minutes)
      cequel[:posts].select_ttl(:title).where(row_keys).first.ttl(:title).
        should be_within(5).of(10.minutes)
    end

    it 'should include timestamp argument' do
      cequel.schema.truncate_table(:posts)
      time = 1.day.ago
      cequel[:posts].insert(row, :timestamp => time)
      cequel[:posts].select_writetime(:title).where(row_keys).
        first.writetime(:title).should == (time.to_f * 1_000_000).to_i
    end

    it 'should include multiple arguments joined by AND' do
      cequel.schema.truncate_table(:posts)
      time = 1.day.ago
      cequel[:posts].insert(row, :ttl => 600, :timestamp => time)
      result = cequel[:posts].select_ttl(:title).select_writetime(:title).
        where(row_keys).first
      result.writetime(:title).should == (time.to_f * 1_000_000).to_i
      result.ttl(:title).should be_within(5).of(10.minutes)
    end
  end

  describe '#update' do
    it 'should send basic update statement' do
      cequel[:posts].where(row_keys).
        update(:title => 'Fun times', :body => 'Fun')
      cequel[:posts].where(row_keys).
        first[:title].should == 'Fun times'
    end

    it 'should send update statement with options' do
      cequel.schema.truncate_table(:posts)
      time = Time.now - 10.minutes

      cequel[:posts].where(row_keys).
        update({title: 'Fun times', body: 'Fun'}, ttl: 600, timestamp: time)

      row = cequel[:posts].
        select_ttl(:title).select_writetime(:title).
        where(row_keys).first

      row.ttl(:title).should be_within(5).of(10.minutes)
      row.writetime(:title).should == (time.to_f * 1_000_000).to_i
    end

    it 'should overwrite list column' do
      cequel[:posts].where(row_keys).
        update(categories: ['Big Data', 'Cassandra'])
      cequel[:posts].where(row_keys).first[:categories].
        should == ['Big Data', 'Cassandra']
    end

    it 'should overwrite set column' do
      cequel[:posts].where(row_keys).update(tags: Set['big-data', 'nosql'])
      cequel[:posts].where(row_keys).first[:tags].
        should == Set['big-data', 'nosql']
    end

    it 'should overwrite map column' do
      time1 = Time.at(Time.now.to_i)
      time2 = Time.at(10.minutes.ago.to_i)
      cequel[:posts].where(row_keys).update(
        trackbacks: {time1 => 'foo', time2 => 'bar'})
      cequel[:posts].where(row_keys).first[:trackbacks].
        should == {time1 => 'foo', time2 => 'bar'}
    end

    it 'should perform various types of update in one go' do
      cequel[:posts].insert(
        row_keys.merge(title: 'Big Data',
                       body: 'Cassandra',
                       categories: ['Scalability']))
      cequel[:posts].where(row_keys).update do
        set(title: 'Bigger Data')
        list_append(:categories, 'Fault-Tolerance')
      end
      cequel[:posts].where(row_keys).first[:title].should == 'Bigger Data'
      cequel[:posts].where(row_keys).first[:categories].
        should == %w(Scalability Fault-Tolerance)
    end
  end

  describe '#list_prepend' do
    it 'should prepend a single element to list column' do
      cequel[:posts].insert(
        row_keys.merge(categories: ['Big Data', 'Cassandra']))
      cequel[:posts].where(row_keys).
        list_prepend(:categories, 'Scalability')
      cequel[:posts].where(row_keys).first[:categories].should ==
        ['Scalability', 'Big Data', 'Cassandra']
    end

    it 'should prepend multiple elements to list column' do
      cequel[:posts].insert(
        row_keys.merge(categories: ['Big Data', 'Cassandra']))
      cequel[:posts].where(row_keys).
        list_prepend(:categories, ['Scalability', 'Partition Tolerance'])
      cequel[:posts].where(row_keys).first[:categories].should ==
        ['Partition Tolerance', 'Scalability', 'Big Data', 'Cassandra']
    end
  end

  describe '#list_append' do
    it 'should append single element to list column' do
      cequel[:posts].insert(
        row_keys.merge(categories: ['Big Data', 'Cassandra']))
      cequel[:posts].where(row_keys).
        list_append(:categories, 'Scalability')
      cequel[:posts].where(row_keys).first[:categories].should ==
        ['Big Data', 'Cassandra', 'Scalability']
    end

    it 'should append multiple elements to list column' do
      cequel[:posts].insert(
        row_keys.merge(categories: ['Big Data', 'Cassandra']))
      cequel[:posts].where(row_keys).
        list_append(:categories, ['Scalability', 'Partition Tolerance'])
      cequel[:posts].where(row_keys).first[:categories].should ==
        ['Big Data', 'Cassandra', 'Scalability', 'Partition Tolerance']
    end
  end

  describe '#list_replace' do
    it 'should add to list at specified index' do
      cequel[:posts].insert(
        row_keys.merge(categories: ['Big Data', 'Cassandra', 'Scalability']))
      cequel[:posts].where(row_keys).
        list_replace(:categories, 1, 'C*')
      cequel[:posts].where(row_keys).first[:categories].should ==
        ['Big Data', 'C*', 'Scalability']
    end
  end

  describe '#list_remove' do
    it 'should remove from list by specified value' do
      cequel[:posts].insert(
        row_keys.merge(categories: ['Big Data', 'Cassandra', 'Scalability']))
      cequel[:posts].where(row_keys).
        list_remove(:categories, 'Cassandra')
      cequel[:posts].where(row_keys).first[:categories].should ==
        ['Big Data', 'Scalability']
    end

    it 'should remove from list by multiple values' do
      cequel[:posts].insert(
        row_keys.merge(categories: ['Big Data', 'Cassandra', 'Scalability']))
      cequel[:posts].where(row_keys).
        list_remove(:categories, ['Big Data', 'Cassandra'])
      cequel[:posts].where(row_keys).first[:categories].should ==
        ['Scalability']
    end
  end

  describe '#set_add' do
    it 'should add one element to set' do
      cequel[:posts].insert(
        row_keys.merge(tags: Set['big-data', 'nosql']))
      cequel[:posts].where(row_keys).set_add(:tags, 'cassandra')
      cequel[:posts].where(row_keys).first[:tags].
        should == Set['big-data', 'nosql', 'cassandra']
    end

    it 'should add multiple elements to set' do
      cequel[:posts].insert(
        row_keys.merge(tags: Set['big-data', 'nosql']))
      cequel[:posts].where(row_keys).set_add(:tags, 'cassandra')
      cequel[:posts].where(row_keys).first[:tags].
        should == Set['big-data', 'nosql', 'cassandra']
    end
  end

  describe '#set_remove' do
    it 'should remove elements from set' do
      cequel[:posts].insert(
        row_keys.merge(tags: Set['big-data', 'nosql', 'cassandra']))
      cequel[:posts].where(row_keys).set_remove(:tags, 'cassandra')
      cequel[:posts].where(row_keys).first[:tags].
        should == Set['big-data', 'nosql']
    end

    it 'should remove multiple elements from set' do
      cequel[:posts].insert(
        row_keys.merge(tags: Set['big-data', 'nosql', 'cassandra']))
      cequel[:posts].where(row_keys).
        set_remove(:tags, Set['nosql', 'cassandra'])
      cequel[:posts].where(row_keys).first[:tags].
        should == Set['big-data']
    end
  end

  describe '#map_update' do
    it 'should update specified map key with value' do
      time1 = Time.at(Time.now.to_i)
      time2 = Time.at(10.minutes.ago.to_i)
      time3 = Time.at(1.hour.ago.to_i)
      cequel[:posts].insert(row_keys.merge(
        trackbacks: {time1 => 'foo', time2 => 'bar'}))
      cequel[:posts].where(row_keys).map_update(:trackbacks, time3 => 'baz')
      cequel[:posts].where(row_keys).first[:trackbacks].
        should == {time1 => 'foo', time2 => 'bar', time3 => 'baz'}
    end

    it 'should update specified map key with multiple values' do
      time1 = Time.at(Time.now.to_i)
      time2 = Time.at(10.minutes.ago.to_i)
      time3 = Time.at(1.hour.ago.to_i)
      cequel[:posts].insert(row_keys.merge(
        trackbacks: {time1 => 'foo', time2 => 'bar'}))
      cequel[:posts].where(row_keys).
        map_update(:trackbacks, time1 => 'FOO', time3 => 'baz')
      cequel[:posts].where(row_keys).first[:trackbacks].
        should == {time1 => 'FOO', time2 => 'bar', time3 => 'baz'}
    end
  end

  describe '#increment' do
    after { cequel.schema.truncate_table(:post_activity) }

    it 'should increment counter columns' do
      cequel[:post_activity].
        where(row_keys).
        increment(visits: 1, tweets: 2)

      row = cequel[:post_activity].where(row_keys).first

      row[:visits].should == 1
      row[:tweets].should == 2
    end
  end

  describe '#decrement' do
    after { cequel.schema.truncate_table(:post_activity) }

    it 'should decrement counter columns' do
      cequel[:post_activity].where(row_keys).
        decrement(visits: 1, tweets: 2)

      row = cequel[:post_activity].where(row_keys).first
      row[:visits].should == -1
      row[:tweets].should == -2
    end
  end

  describe '#delete' do
    before do
      cequel[:posts].
        insert(row_keys.merge(title: 'Big Data', body: 'It\'s big.'))
    end

    it 'should send basic delete statement' do
      cequel[:posts].where(row_keys).delete
      cequel[:posts].where(row_keys).first.should be_nil
    end

    it 'should send delete statement for specified columns' do
      cequel[:posts].where(row_keys).delete(:body)
      row = cequel[:posts].where(row_keys).first
      row[:body].should be_nil
      row[:title].should == 'Big Data'
    end

    it 'should send delete statement with writetime option' do
      time = Time.now - 10.minutes

      cequel[:posts].where(row_keys).delete(
        :body, :timestamp => time
      )
      row = cequel[:posts].select(:body).where(row_keys).first
      row[:body].should == 'It\'s big.'
      # This means timestamp is working, since the earlier timestamp would cause
      # Cassandra to ignore the deletion
    end
  end

  describe '#list_remove_at' do
    it 'should remove element at specified position from list' do
      cequel[:posts].
        insert(row_keys.merge(categories: ['Big Data', 'NoSQL', 'Cassandra']))
      cequel[:posts].where(row_keys).list_remove_at(:categories, 1)
      cequel[:posts].where(row_keys).first[:categories].
        should == ['Big Data', 'Cassandra']
    end

    it 'should remove element at specified positions from list' do
      cequel[:posts].
        insert(row_keys.merge(categories: ['Big Data', 'NoSQL', 'Cassandra']))
      cequel[:posts].where(row_keys).list_remove_at(:categories, 0, 2)
      cequel[:posts].where(row_keys).first[:categories].
        should == ['NoSQL']
    end
  end

  describe '#map_remove' do
    it 'should remove one element from a map' do
      time1 = Time.at(Time.now.to_i)
      time2 = Time.at(10.minutes.ago.to_i)
      time3 = Time.at(1.hour.ago.to_i)
      cequel[:posts].insert(row_keys.merge(
        trackbacks: {time1 => 'foo', time2 => 'bar', time3 => 'baz'}))
      cequel[:posts].where(row_keys).map_remove(:trackbacks, time2)
      cequel[:posts].where(row_keys).first[:trackbacks].
        should == {time1 => 'foo', time3 => 'baz'}
    end

    it 'should remove multiple elements from a map' do
      time1 = Time.at(Time.now.to_i)
      time2 = Time.at(10.minutes.ago.to_i)
      time3 = Time.at(1.hour.ago.to_i)
      cequel[:posts].insert(row_keys.merge(
        trackbacks: {time1 => 'foo', time2 => 'bar', time3 => 'baz'}))
      cequel[:posts].where(row_keys).map_remove(:trackbacks, time1, time3)
      cequel[:posts].where(row_keys).first[:trackbacks].
        should == {time2 => 'bar'}
    end
  end

  describe '#cql' do
    it 'should generate select statement with all columns' do
      cequel[:posts].cql.should == ['SELECT * FROM posts']
    end
  end

  describe '#select' do
    before do
      cequel[:posts].insert(row_keys.merge(
        title: 'Big Data',
        body: 'Fault Tolerance',
        published_at: Time.now
      ))
    end

    it 'should generate select statement with given columns' do
      cequel[:posts].select(:title, :body).where(row_keys).first.
        keys.should == %w(title body)
    end

    it 'should accept array argument' do
      cequel[:posts].select([:title, :body]).where(row_keys).first.
        keys.should == %w(title body)
    end

    it 'should combine multiple selects' do
      cequel[:posts].select(:title).select(:body).where(row_keys).first.
        keys.should == %w(title body)
    end
  end

  describe '#select!' do
    before do
      cequel[:posts].insert(row_keys.merge(
        title: 'Big Data',
        body: 'Fault Tolerance',
        published_at: Time.now
      ))
    end

    it 'should override select statement with given columns' do
      cequel[:posts].select(:title, :body).select!(:published_at).
        where(row_keys).first.keys.should == %w(published_at)
    end
  end

  describe '#where' do
    before do
      cequel[:posts].insert(row_keys.merge(
        title: 'Big Data',
        body: 'Fault Tolerance',
        published_at: Time.now
      ))
    end

    it 'should build WHERE statement from hash' do
      cequel[:posts].where(blog_subdomain: row_keys[:blog_subdomain]).
        first[:title].should == 'Big Data'
      cequel[:posts].where(blog_subdomain: 'foo').first.should be_nil
    end

    it 'should build WHERE statement from multi-element hash' do
      cequel[:posts].where(row_keys).first[:title].should == 'Big Data'
      cequel[:posts].where(row_keys.merge(:permalink => 'foo')).
        first.should be_nil
    end

    it 'should build WHERE statement with IN' do
      cequel[:posts].insert(row_keys.merge(
        blog_subdomain: 'big-data-weekly',
        title: 'Cassandra',
      ))
      cequel[:posts].insert(row_keys.merge(
        blog_subdomain: 'bogus-blog',
        title: 'Bogus Post',
      ))
      cequel[:posts].where(
        :blog_subdomain => %w(cassandra big-data-weekly),
        :permalink => 'big-data'
      ).map { |row| row[:title] }.should == ['Big Data', 'Cassandra']
    end

    it 'should use = if provided one-element array' do
      cequel[:posts].
        where(row_keys.merge(blog_subdomain: [row_keys[:blog_subdomain]])).
        first[:title].should == 'Big Data'
    end

    it 'should build WHERE statement from CQL string' do
      cequel[:posts].where("blog_subdomain = '#{row_keys[:blog_subdomain]}'").
        first[:title].should == 'Big Data'
    end

    it 'should build WHERE statement from CQL string with bind variables' do
      cequel[:posts].where("blog_subdomain = ?", row_keys[:blog_subdomain]).
        first[:title].should == 'Big Data'
    end

    it 'should aggregate multiple WHERE statements' do
      cequel[:posts].where(:blog_subdomain => row_keys[:blog_subdomain]).
        where('permalink = ?', row_keys[:permalink]).
        first[:title].should == 'Big Data'
    end

  end

  describe '#where!' do
    before do
      cequel[:posts].insert(row_keys.merge(
        title: 'Big Data',
        body: 'Fault Tolerance',
        published_at: Time.now
      ))
    end

    it 'should override chained conditions' do
      cequel[:posts].where(:permalink => 'bogus').
        where!(:blog_subdomain => row_keys[:blog_subdomain]).
        first[:title].should == 'Big Data'
    end
  end

  describe '#limit' do
    before do
      cequel[:posts].insert(row_keys.merge(title: 'Big Data'))
      cequel[:posts].insert(
        row_keys.merge(permalink: 'marshmallows', title: 'Marshmallows'))
      cequel[:posts].insert(
        row_keys.merge(permalink: 'zz-top', title: 'ZZ Top'))
    end

    it 'should add LIMIT' do
      cequel[:posts].where(row_keys.slice(:blog_subdomain)).limit(2).
        map { |row| row[:title] }.should == ['Big Data', 'Marshmallows']
    end
  end

  describe '#order' do
    before do
      cequel[:posts].insert(row_keys.merge(title: 'Big Data'))
      cequel[:posts].insert(
        row_keys.merge(permalink: 'marshmallows', title: 'Marshmallows'))
      cequel[:posts].insert(
        row_keys.merge(permalink: 'zz-top', title: 'ZZ Top'))
    end

    it 'should add order' do
      cequel[:posts].where(row_keys.slice(:blog_subdomain)).
        order(permalink: 'desc').map { |row| row[:title] }.
        should == ['ZZ Top', 'Marshmallows', 'Big Data']
    end
  end

  describe 'result enumeration' do
    let(:row) { row_keys.merge(:title => 'Big Data') }

    before do
      cequel[:posts].insert(row)
    end

    it 'should enumerate over results' do
      cequel[:posts].to_a.map { |row| row.select { |k, v| v }}.
        should == [row.stringify_keys]
    end

    it 'should provide results with indifferent access' do
      cequel[:posts].to_a.first[:blog_permalink].
        should == row_keys[:blog_permalink]
    end

    it 'should not run query if no block given to #each' do
      expect { cequel[:posts].each }.to_not raise_error
    end

    it 'should return Enumerator if no block given to #each' do
      cequel[:posts].each.each_with_index.
        map { |row, i| [row[:blog_permalink], i] }.
        should == [[row[:blog_permalink], 0]]
    end
  end

  describe '#first' do
    let(:row) { row_keys.merge(:title => 'Big Data') }

    before do
      cequel[:posts].insert(row)
      cequel[:posts].insert(
        row_keys.merge(:permalink => 'zz-top', :title => 'ZZ Top'))
    end

    it 'should run a query with LIMIT 1 and return first row' do
      cequel[:posts].first.select { |k, v| v }.should == row.stringify_keys
    end
  end

  describe '#count' do
    before do
      4.times do |i|
        cequel[:posts].insert(row_keys.merge(
          permalink: "post-#{i}", title: "Post #{i}"))
      end
    end

    it 'should run a count query and return count' do
      cequel[:posts].count.should == 4
    end

    it 'should use where clause if specified' do
      cequel[:posts].where(row_keys.merge(permalink: 'post-1')).
        count.should == 1
    end

    it 'should use limit if specified' do
      cequel[:posts].limit(2).count.should == 2
    end
  end

end
