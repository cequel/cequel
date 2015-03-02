# -*- encoding : utf-8 -*-
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
      expect(cequel[:posts].where(row_keys).first[:title]).to eq('Fun times')
    end

    it 'should correctly insert a list' do
      cequel[:posts].insert(row)
      expect(cequel[:posts].where(row_keys).first[:categories]).
        to eq(['Fun', 'Profit'])
    end

    it 'should correctly insert a set' do
      cequel[:posts].insert(row)
      expect(cequel[:posts].where(row_keys).first[:tags]).
        to eq(Set['cassandra', 'big-data'])
    end

    it 'should correctly insert a map' do
      cequel[:posts].insert(row)
      expect(cequel[:posts].where(row_keys).first[:trackbacks]).
        to eq(row[:trackbacks])
    end

    it 'should include ttl argument' do
      cequel[:posts].insert(row, :ttl => 10.minutes)
      expect(cequel[:posts].select_ttl(:title).where(row_keys).first.ttl(:title)).
        to be_within(5).of(10.minutes)
    end

    it 'should include timestamp argument' do
      cequel.schema.truncate_table(:posts)
      time = 1.day.ago
      cequel[:posts].insert(row, :timestamp => time)
      expect(cequel[:posts].select_writetime(:title).where(row_keys).
        first.writetime(:title)).to eq((time.to_f * 1_000_000).to_i)
    end

    it 'should insert row with given consistency' do
      expect_query_with_consistency(/INSERT/, :one) do
        cequel[:posts].insert(row, consistency: :one)
      end
    end

    it 'should include multiple arguments joined by AND' do
      cequel.schema.truncate_table(:posts)
      time = 1.day.ago
      cequel[:posts].insert(row, :ttl => 600, :timestamp => time)
      result = cequel[:posts].select_ttl(:title).select_writetime(:title).
        where(row_keys).first
      expect(result.writetime(:title)).to eq((time.to_f * 1_000_000).to_i)
      expect(result.ttl(:title)).to be_within(5).of(10.minutes)
    end
  end

  describe '#update' do
    it 'should send basic update statement' do
      cequel[:posts].where(row_keys).
        update(:title => 'Fun times', :body => 'Fun')
      expect(cequel[:posts].where(row_keys).
        first[:title]).to eq('Fun times')
    end

    it 'should send update statement with options' do
      cequel.schema.truncate_table(:posts)
      time = Time.now - 10.minutes

      cequel[:posts].where(row_keys).
        update({title: 'Fun times', body: 'Fun'}, ttl: 600, timestamp: time)

      row = cequel[:posts].
        select_ttl(:title).select_writetime(:title).
        where(row_keys).first

      expect(row.ttl(:title)).to be_within(5).of(10.minutes)
      expect(row.writetime(:title)).to eq((time.to_f * 1_000_000).to_i)
    end

    it 'should send update statement with given consistency' do
      expect_query_with_consistency(/UPDATE/, :one) do
        cequel[:posts].where(row_keys).update(
          {title: 'Marshmallows'}, consistency: :one)
      end
    end

    it 'should overwrite list column' do
      cequel[:posts].where(row_keys).
        update(categories: ['Big Data', 'Cassandra'])
      expect(cequel[:posts].where(row_keys).first[:categories]).
        to eq(['Big Data', 'Cassandra'])
    end

    it 'should overwrite set column' do
      cequel[:posts].where(row_keys).update(tags: Set['big-data', 'nosql'])
      expect(cequel[:posts].where(row_keys).first[:tags]).
        to eq(Set['big-data', 'nosql'])
    end

    it 'should overwrite map column' do
      time1 = Time.at(Time.now.to_i)
      time2 = Time.at(10.minutes.ago.to_i)
      cequel[:posts].where(row_keys).update(
        trackbacks: {time1 => 'foo', time2 => 'bar'})
      expect(cequel[:posts].where(row_keys).first[:trackbacks]).
        to eq({time1 => 'foo', time2 => 'bar'})
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
      expect(cequel[:posts].where(row_keys).first[:title]).to eq('Bigger Data')
      expect(cequel[:posts].where(row_keys).first[:categories]).
        to eq(%w(Scalability Fault-Tolerance))
    end

    it 'should use the last value set for a given column' do
      cequel[:posts].insert(
        row_keys.merge(title: 'Big Data',
                       body: 'Cassandra',
                       categories: ['Scalability']))
      cequel[:posts].where(row_keys).update do
        set(title: 'Bigger Data')
        set(title: 'Even Bigger Data')
      end
      expect(cequel[:posts].where(row_keys).first[:title]).to eq('Even Bigger Data')
    end
  end

  describe '#list_prepend' do
    it 'should prepend a single element to list column' do
      cequel[:posts].insert(
        row_keys.merge(categories: ['Big Data', 'Cassandra']))
      cequel[:posts].where(row_keys).
        list_prepend(:categories, 'Scalability')
      expect(cequel[:posts].where(row_keys).first[:categories]).to eq(
        ['Scalability', 'Big Data', 'Cassandra']
      )
    end

    it 'should prepend multiple elements to list column' do
      cequel[:posts].insert(
        row_keys.merge(categories: ['Big Data', 'Cassandra']))
      cequel[:posts].where(row_keys).
        list_prepend(:categories, ['Scalability', 'Partition Tolerance'])
      expect(cequel[:posts].where(row_keys).first[:categories]).to eq(
        ['Partition Tolerance', 'Scalability', 'Big Data', 'Cassandra']
      )
    end
  end

  describe '#list_append' do
    it 'should append single element to list column' do
      cequel[:posts].insert(
        row_keys.merge(categories: ['Big Data', 'Cassandra']))
      cequel[:posts].where(row_keys).
        list_append(:categories, 'Scalability')
      expect(cequel[:posts].where(row_keys).first[:categories]).to eq(
        ['Big Data', 'Cassandra', 'Scalability']
      )
    end

    it 'should append multiple elements to list column' do
      cequel[:posts].insert(
        row_keys.merge(categories: ['Big Data', 'Cassandra']))
      cequel[:posts].where(row_keys).
        list_append(:categories, ['Scalability', 'Partition Tolerance'])
      expect(cequel[:posts].where(row_keys).first[:categories]).to eq(
        ['Big Data', 'Cassandra', 'Scalability', 'Partition Tolerance']
      )
    end
  end

  describe '#list_replace' do
    it 'should add to list at specified index' do
      cequel[:posts].insert(
        row_keys.merge(categories: ['Big Data', 'Cassandra', 'Scalability']))
      cequel[:posts].where(row_keys).
        list_replace(:categories, 1, 'C*')
      expect(cequel[:posts].where(row_keys).first[:categories]).to eq(
        ['Big Data', 'C*', 'Scalability']
      )
    end
  end

  describe '#list_remove' do
    it 'should remove from list by specified value' do
      cequel[:posts].insert(
        row_keys.merge(categories: ['Big Data', 'Cassandra', 'Scalability']))
      cequel[:posts].where(row_keys).
        list_remove(:categories, 'Cassandra')
      expect(cequel[:posts].where(row_keys).first[:categories]).to eq(
        ['Big Data', 'Scalability']
      )
    end

    it 'should remove from list by multiple values' do
      cequel[:posts].insert(
        row_keys.merge(categories: ['Big Data', 'Cassandra', 'Scalability']))
      cequel[:posts].where(row_keys).
        list_remove(:categories, ['Big Data', 'Cassandra'])
      expect(cequel[:posts].where(row_keys).first[:categories]).to eq(
        ['Scalability']
      )
    end
  end

  describe '#set_add' do
    it 'should add one element to set' do
      cequel[:posts].insert(
        row_keys.merge(tags: Set['big-data', 'nosql']))
      cequel[:posts].where(row_keys).set_add(:tags, 'cassandra')
      expect(cequel[:posts].where(row_keys).first[:tags]).
        to eq(Set['big-data', 'nosql', 'cassandra'])
    end

    it 'should add multiple elements to set' do
      cequel[:posts].insert(
        row_keys.merge(tags: Set['big-data', 'nosql']))
      cequel[:posts].where(row_keys).set_add(:tags, 'cassandra')
      expect(cequel[:posts].where(row_keys).first[:tags]).
        to eq(Set['big-data', 'nosql', 'cassandra'])
    end
  end

  describe '#set_remove' do
    it 'should remove elements from set' do
      cequel[:posts].insert(
        row_keys.merge(tags: Set['big-data', 'nosql', 'cassandra']))
      cequel[:posts].where(row_keys).set_remove(:tags, 'cassandra')
      expect(cequel[:posts].where(row_keys).first[:tags]).
        to eq(Set['big-data', 'nosql'])
    end

    it 'should remove multiple elements from set' do
      cequel[:posts].insert(
        row_keys.merge(tags: Set['big-data', 'nosql', 'cassandra']))
      cequel[:posts].where(row_keys).
        set_remove(:tags, Set['nosql', 'cassandra'])
      expect(cequel[:posts].where(row_keys).first[:tags]).
        to eq(Set['big-data'])
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
      expect(cequel[:posts].where(row_keys).first[:trackbacks]).
        to eq({time1 => 'foo', time2 => 'bar', time3 => 'baz'})
    end

    it 'should update specified map key with multiple values' do
      time1 = Time.at(Time.now.to_i)
      time2 = Time.at(10.minutes.ago.to_i)
      time3 = Time.at(1.hour.ago.to_i)
      cequel[:posts].insert(row_keys.merge(
        trackbacks: {time1 => 'foo', time2 => 'bar'}))
      cequel[:posts].where(row_keys).
        map_update(:trackbacks, time1 => 'FOO', time3 => 'baz')
      expect(cequel[:posts].where(row_keys).first[:trackbacks]).
        to eq({time1 => 'FOO', time2 => 'bar', time3 => 'baz'})
    end
  end

  describe '#increment' do
    after { cequel.schema.truncate_table(:post_activity) }

    it 'should increment counter columns' do
      cequel[:post_activity].
        where(row_keys).
        increment(visits: 1, tweets: 2)

      row = cequel[:post_activity].where(row_keys).first

      expect(row[:visits]).to eq(1)
      expect(row[:tweets]).to eq(2)
    end
  end

  describe '#decrement' do
    after { cequel.schema.truncate_table(:post_activity) }

    it 'should decrement counter columns' do
      cequel[:post_activity].where(row_keys).
        decrement(visits: 1, tweets: 2)

      row = cequel[:post_activity].where(row_keys).first
      expect(row[:visits]).to eq(-1)
      expect(row[:tweets]).to eq(-2)
    end
  end

  describe '#delete' do
    before do
      cequel[:posts].
        insert(row_keys.merge(title: 'Big Data', body: 'It\'s big.'))
    end

    it 'should send basic delete statement' do
      cequel[:posts].where(row_keys).delete
      expect(cequel[:posts].where(row_keys).first).to be_nil
    end

    it 'should send delete statement for specified columns' do
      cequel[:posts].where(row_keys).delete(:body)
      row = cequel[:posts].where(row_keys).first
      expect(row[:body]).to be_nil
      expect(row[:title]).to eq('Big Data')
    end

    it 'should send delete statement with writetime option' do
      time = Time.now - 10.minutes

      cequel[:posts].where(row_keys).delete(
        :body, :timestamp => time
      )
      row = cequel[:posts].select(:body).where(row_keys).first
      expect(row[:body]).to eq('It\'s big.')
      # This means timestamp is working, since the earlier timestamp would cause
      # Cassandra to ignore the deletion
    end

    it 'should send delete with specified consistency' do
      expect_query_with_consistency(/DELETE/, :one) do
        cequel[:posts].where(row_keys).delete(:body, :consistency => :one)
      end
    end
  end

  describe '#list_remove_at' do
    it 'should remove element at specified position from list' do
      cequel[:posts].
        insert(row_keys.merge(categories: ['Big Data', 'NoSQL', 'Cassandra']))
      cequel[:posts].where(row_keys).list_remove_at(:categories, 1)
      expect(cequel[:posts].where(row_keys).first[:categories]).
        to eq(['Big Data', 'Cassandra'])
    end

    it 'should remove element at specified positions from list' do
      cequel[:posts].
        insert(row_keys.merge(categories: ['Big Data', 'NoSQL', 'Cassandra']))
      cequel[:posts].where(row_keys).list_remove_at(:categories, 0, 2)
      expect(cequel[:posts].where(row_keys).first[:categories]).
        to eq(['NoSQL'])
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
      expect(cequel[:posts].where(row_keys).first[:trackbacks]).
        to eq({time1 => 'foo', time3 => 'baz'})
    end

    it 'should remove multiple elements from a map' do
      time1 = Time.at(Time.now.to_i)
      time2 = Time.at(10.minutes.ago.to_i)
      time3 = Time.at(1.hour.ago.to_i)
      cequel[:posts].insert(row_keys.merge(
        trackbacks: {time1 => 'foo', time2 => 'bar', time3 => 'baz'}))
      cequel[:posts].where(row_keys).map_remove(:trackbacks, time1, time3)
      expect(cequel[:posts].where(row_keys).first[:trackbacks]).
        to eq({time2 => 'bar'})
    end
  end

  describe '#cql' do
    it 'should generate select statement with all columns' do
      expect(cequel[:posts].cql).to eq(['SELECT * FROM posts'])
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
      expect(cequel[:posts].select(:title, :body).where(row_keys).first.
        keys).to eq(%w(title body))
    end

    it 'should accept array argument' do
      expect(cequel[:posts].select([:title, :body]).where(row_keys).first.
        keys).to eq(%w(title body))
    end

    it 'should combine multiple selects' do
      expect(cequel[:posts].select(:title).select(:body).where(row_keys).first.
        keys).to eq(%w(title body))
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
      expect(cequel[:posts].select(:title, :body).select!(:published_at).
        where(row_keys).first.keys).to eq(%w(published_at))
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
      expect(cequel[:posts].where(blog_subdomain: row_keys[:blog_subdomain]).
        first[:title]).to eq('Big Data')
      expect(cequel[:posts].where(blog_subdomain: 'foo').first).to be_nil
    end

    it 'should build WHERE statement from multi-element hash' do
      expect(cequel[:posts].where(row_keys).first[:title]).to eq('Big Data')
      expect(cequel[:posts].where(row_keys.merge(:permalink => 'foo')).
        first).to be_nil
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
      expect(cequel[:posts].where(
        :blog_subdomain => %w(cassandra big-data-weekly),
        :permalink => 'big-data'
      ).map { |row| row[:title] }).to eq(['Big Data', 'Cassandra'])
    end

    it 'should use = if provided one-element array' do
      expect(cequel[:posts].
        where(row_keys.merge(blog_subdomain: [row_keys[:blog_subdomain]])).
        first[:title]).to eq('Big Data')
    end

    it 'should build WHERE statement from CQL string' do
      expect(cequel[:posts].where("blog_subdomain = '#{row_keys[:blog_subdomain]}'").
        first[:title]).to eq('Big Data')
    end

    it 'should build WHERE statement from CQL string with bind variables' do
      expect(cequel[:posts].where("blog_subdomain = ?", row_keys[:blog_subdomain]).
        first[:title]).to eq('Big Data')
    end

    it 'should aggregate multiple WHERE statements' do
      expect(cequel[:posts].where(:blog_subdomain => row_keys[:blog_subdomain]).
        where('permalink = ?', row_keys[:permalink]).
        first[:title]).to eq('Big Data')
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
      expect(cequel[:posts].where(:permalink => 'bogus').
        where!(:blog_subdomain => row_keys[:blog_subdomain]).
        first[:title]).to eq('Big Data')
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
      expect(cequel[:posts].where(row_keys.slice(:blog_subdomain)).limit(2).
        map { |row| row[:title] }).to eq(['Big Data', 'Marshmallows'])
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
      expect(cequel[:posts].where(row_keys.slice(:blog_subdomain)).
        order(permalink: 'desc').map { |row| row[:title] }).
        to eq(['ZZ Top', 'Marshmallows', 'Big Data'])
    end
  end

  describe '#consistency' do
    let(:data_set) { cequel[:posts].consistency(:one) }

    it 'should issue SELECT with scoped consistency' do
      expect_query_with_consistency(/SELECT/, :one) { data_set.to_a }
    end

    it 'should issue COUNT with scoped consistency' do
      expect_query_with_consistency(/SELECT.*COUNT/, :one) { data_set.count }
    end

    it 'should issue INSERT with scoped consistency' do
      expect_query_with_consistency(/INSERT/, :one) do
        data_set.insert(row_keys)
      end
    end

    it 'should issue UPDATE with scoped consistency' do
      expect_query_with_consistency(/UPDATE/, :one) do
        data_set.where(row_keys).update(title: 'Marshmallows')
      end
    end

    it 'should issue DELETE with scoped consistency' do
      expect_query_with_consistency(/DELETE/, :one) do
        data_set.where(row_keys).delete
      end
    end

    it 'should issue DELETE column with scoped consistency' do
      expect_query_with_consistency(/DELETE/, :one) do
        data_set.where(row_keys).delete(:title)
      end
    end
  end

  describe 'default consistency' do
    before(:all) { cequel.default_consistency = :all }
    after(:all) { cequel.default_consistency = nil }
    let(:data_set) { cequel[:posts] }

    it 'should issue SELECT with default consistency' do
      expect_query_with_consistency(/SELECT/, :all) { data_set.to_a }
    end

    it 'should issue COUNT with default consistency' do
      expect_query_with_consistency(/SELECT.*COUNT/, :all) { data_set.count }
    end

    it 'should issue INSERT with default consistency' do
      expect_query_with_consistency(/INSERT/, :all) do
        data_set.insert(row_keys)
      end
    end

    it 'should issue UPDATE with default consistency' do
      expect_query_with_consistency(/UPDATE/, :all) do
        data_set.where(row_keys).update(title: 'Marshmallows')
      end
    end

    it 'should issue DELETE with default consistency' do
      expect_query_with_consistency(/DELETE/, :all) do
        data_set.where(row_keys).delete
      end
    end

    it 'should issue DELETE column with default consistency' do
      expect_query_with_consistency(/DELETE/, :all) do
        data_set.where(row_keys).delete(:title)
      end
    end
  end

  describe 'result enumeration' do
    let(:row) { row_keys.merge(:title => 'Big Data') }

    before do
      cequel[:posts].insert(row)
    end

    it 'should enumerate over results' do
      expect(cequel[:posts].to_a.map { |row| row.select { |k, v| v }}).
        to eq([row.stringify_keys])
    end

    it 'should provide results with indifferent access' do
      expect(cequel[:posts].to_a.first[:blog_permalink]).
        to eq(row_keys[:blog_permalink])
    end

    it 'should not run query if no block given to #each' do
      expect { cequel[:posts].each }.to_not raise_error
    end

    it 'should return Enumerator if no block given to #each' do
      expect(cequel[:posts].each.each_with_index.
        map { |row, i| [row[:blog_permalink], i] }).
        to eq([[row[:blog_permalink], 0]])
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
      expect(cequel[:posts].first.select { |k, v| v }).to eq(row.stringify_keys)
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
      expect(cequel[:posts].count).to eq(4)
    end

    it 'should use where clause if specified' do
      expect(cequel[:posts].where(row_keys.merge(permalink: 'post-1')).
        count).to eq(1)
    end

    it 'should use limit if specified' do
      expect(cequel[:posts].limit(2).count).to eq(2)
    end
  end

  describe "columns with capital letters and periods" do
    before :each do
      cequel.schema.create_table(:legacy_posts) do
        key :"Blog_subdomain.column", :text
        key :"Permalink.column", :text
        column :"Title.column", :text
        column :"Body.column", :text
        column :"Published_at.column", :timestamp
        list :"Categories.column", :text
        set :"Tags.column", :text
        map :"Trackbacks.column", :timestamp, :text
      end
      cequel.schema.create_table(:legacy_post_activity) do
        key :"Blog_subdomain.column", :text
        key :"Permalink.column", :text
        column :"Visits.column", :counter
        column :"Tweets.column", :counter
      end
    end

    after :each do
      cequel.schema.drop_table(:legacy_posts)
      cequel.schema.drop_table(:legacy_post_activity)
    end

    let(:now) { Time.now }
    let(:epoch) { Time.at(0) }

    let(:row_keys) do
      {
        :"Blog_subdomain.column" => "blog_subdomain",
        :"Permalink.column" => "permalink",
      }
    end

    let(:post_fields) do
      row_keys.merge({
        :"Title.column" => "title",
        :"Body.column" => "body",
        :"Published_at.column" => now,
        :"Categories.column" => ["category1", "category2"],
        :"Tags.column" => Set["tag1", "tag2"],
        :"Trackbacks.column" => { now => 'www.google.com' }
      })
    end

    let(:non_key_update_fields) do
      {
        :"Title.column" => "title updated",
        :"Body.column" => "body updated",
        :"Published_at.column" => epoch,
        :"Categories.column" => ["category1 updated", "category2"],
        :"Tags.column" => Set["tag1 updated", "tag2"],
        :"Trackbacks.column" => {epoch => 'www.google.com'}
      }
    end

    let(:post_fields_updated) do
      row_keys.merge(non_key_update_fields)
    end

    let(:post_activity_fields) do
      row_keys.merge({
        :"Visits.column" => 5,
        :"Tweets.column" => 5
      })
    end

    it 'should insert when columns have capital letters and periods' do
      cequel[:legacy_posts].insert(post_fields)
      row = cequel[:legacy_posts].where(row_keys).first
      expect(
        row.each_with_object({}) {|(k,v), hash| hash[k.to_s] = v.inspect}
      ).to eq(
        post_fields.each_with_object({}) {|(k,v), hash| hash[k.to_s] = v.inspect}
      )
    end

    it "should update" do
      cequel[:legacy_posts].insert(post_fields)
      cequel[:legacy_posts].where(row_keys).
        update(non_key_update_fields)

      row = cequel[:legacy_posts].where(row_keys).first
      expect(
        row.each_with_object({}) {|(k,v), hash| hash[k.to_s] = v.inspect}
      ).to eq(
        post_fields_updated.each_with_object({}) {|(k,v), hash| hash[k.to_s] = v.inspect}
      )
    end

    it "should delete from list" do
      cequel[:legacy_posts].insert(post_fields)
      cequel[:legacy_posts].
        where(row_keys).
        list_remove_at(:"Categories.column", 0)
      row = cequel[:legacy_posts].where(row_keys).first
      expect(row[:"Categories.column"]).to eq ["category2"]
    end

    it "should preprend to list" do
      cequel[:legacy_posts].insert(post_fields)
      cequel[:legacy_posts].
        where(row_keys).
        list_prepend(:"Categories.column", ["category0"])
      row = cequel[:legacy_posts].where(row_keys).first
      expect(row[:"Categories.column"]).to eq ["category0", "category1", "category2"]
    end

    it "should append to list" do
      cequel[:legacy_posts].insert(post_fields)
      cequel[:legacy_posts].
        where(row_keys).
        list_append(:"Categories.column", ["category3"])
      row = cequel[:legacy_posts].where(row_keys).first
      expect(row[:"Categories.column"]).to eq ["category1", "category2", "category3"]
    end

    it "should remove from list" do
      cequel[:legacy_posts].insert(post_fields)
      cequel[:legacy_posts].
        where(row_keys).
        list_remove(:"Categories.column", ["category2"])
      row = cequel[:legacy_posts].where(row_keys).first
      expect(row[:"Categories.column"]).to eq ["category1"]
    end

    it "should replace from list" do
      cequel[:legacy_posts].insert(post_fields)
      cequel[:legacy_posts].
        where(row_keys).
        list_replace(:"Categories.column", 0, "category0")
      row = cequel[:legacy_posts].where(row_keys).first
      expect(row[:"Categories.column"]).to eq ["category0", "category2"]
    end

    it "should delete from set" do
      cequel[:legacy_posts].insert(post_fields)
      cequel[:legacy_posts].
        where(row_keys).
        set_remove(:"Tags.column", "tag1")
      row = cequel[:legacy_posts].where(row_keys).first
      expect(row[:"Tags.column"]).to eq Set["tag2"]
    end

    it "should add to set" do
      cequel[:legacy_posts].insert(post_fields)
      cequel[:legacy_posts].
        where(row_keys).
        set_add(:"Tags.column", "tag3")
      row = cequel[:legacy_posts].where(row_keys).first
      expect(row[:"Tags.column"]).to eq Set["tag1", "tag2", "tag3"]
    end

    it "should delete from map" do
      cequel[:legacy_posts].insert(post_fields)
      cequel[:legacy_posts].
        where(row_keys).
        map_remove(:"Trackbacks.column", now)
      row = cequel[:legacy_posts].where(row_keys).first

      expect(row[:"Trackbacks.column"]).to be_nil
    end

    it "should update map" do
      cequel[:legacy_posts].insert(post_fields)
      cequel[:legacy_posts].
        where(row_keys).
        map_update(:"Trackbacks.column", {now => "moc.elgoog.www"})
      row = cequel[:legacy_posts].where(row_keys).first

      expect(row[:"Trackbacks.column"].size).to eq 1
      expect(row[:"Trackbacks.column"].keys.first.to_i).to eq now.to_i
      expect(row[:"Trackbacks.column"].values.first).to eq "moc.elgoog.www"
    end

    it 'should increment' do
      cequel[:legacy_post_activity].
        where(row_keys).
        increment(:"Visits.column" => 1, :"Tweets.column" => 2)

      row = cequel[:legacy_post_activity].where(row_keys).first

      expect(row[:"Visits.column"]).to eq(1)
      expect(row[:"Tweets.column"]).to eq(2)
    end

    it 'should include ttl argument' do
      cequel[:legacy_posts].insert(post_fields, :ttl => 10.minutes)
      expect(cequel[:legacy_posts].select_ttl(:"Title.column").where(row_keys).first.ttl(:"Title.column")).
        to be_within(5).of(10.minutes)
    end

    it 'should include timestamp argument' do
      cequel.schema.truncate_table(:legacy_posts)
      time = 1.day.ago
      cequel[:legacy_posts].insert(post_fields, :timestamp => time)
      expect(cequel[:legacy_posts].select_writetime(:"Title.column").where(row_keys).
        first.writetime(:"Title.column")).to eq((time.to_f * 1_000_000).to_i)
    end
  end

end
