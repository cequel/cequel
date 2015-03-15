# -*- encoding : utf-8 -*-
require File.expand_path('../../spec_helper', __FILE__)

describe Cequel::Schema::TableReader do

  after do
    cequel.schema.drop_table(:posts)
  end

  let(:table) { cequel.schema.read_table(:posts) }

  describe 'reading simple key' do
    before do
      cequel.execute("CREATE TABLE posts (permalink text PRIMARY KEY)")
    end

    it 'should read name correctly' do
      expect(table.partition_key_columns.first.name).to eq(:permalink)
    end

    it 'should read type correctly' do
      expect(table.partition_key_columns.first.type).to be_a(Cequel::Type::Text)
    end

    it 'should have no nonpartition keys' do
      expect(table.clustering_columns).to be_empty
    end
  end # describe 'reading simple key'

  describe 'reading single non-partition key' do
    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (
          blog_subdomain text,
          permalink ascii,
          PRIMARY KEY (blog_subdomain, permalink)
        )
      CQL
    end

    it 'should read partition key name' do
      expect(table.partition_key_columns.map(&:name)).to eq([:blog_subdomain])
    end

    it 'should read partition key type' do
      expect(table.partition_key_columns.map(&:type)).to eq([Cequel::Type::Text.instance])
    end

    it 'should read non-partition key name' do
      expect(table.clustering_columns.map(&:name)).to eq([:permalink])
    end

    it 'should read non-partition key type' do
      expect(table.clustering_columns.map(&:type)).
        to eq([Cequel::Type::Ascii.instance])
    end

    it 'should default clustering order to asc' do
      expect(table.clustering_columns.map(&:clustering_order)).to eq([:asc])
    end
  end # describe 'reading single non-partition key'

  describe 'reading reverse-ordered non-partition key' do
    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (
          blog_subdomain text,
          permalink ascii,
          PRIMARY KEY (blog_subdomain, permalink)
        )
        WITH CLUSTERING ORDER BY (permalink DESC)
      CQL
    end

    it 'should read non-partition key name' do
      expect(table.clustering_columns.map(&:name)).to eq([:permalink])
    end

    it 'should read non-partition key type' do
      expect(table.clustering_columns.map(&:type)).
        to eq([Cequel::Type::Ascii.instance])
    end

    it 'should recognize reversed clustering order' do
      expect(table.clustering_columns.map(&:clustering_order)).to eq([:desc])
    end
  end # describe 'reading reverse-ordered non-partition key'

  describe 'reading compound non-partition key' do
    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (
          blog_subdomain text,
          permalink ascii,
          author_id uuid,
          PRIMARY KEY (blog_subdomain, permalink, author_id)
        )
        WITH CLUSTERING ORDER BY (permalink DESC, author_id ASC)
      CQL
    end

    it 'should read non-partition key names' do
      expect(table.clustering_columns.map(&:name)).to eq([:permalink, :author_id])
    end

    it 'should read non-partition key types' do
      expect(table.clustering_columns.map(&:type)).
        to eq([Cequel::Type::Ascii.instance, Cequel::Type::Uuid.instance])
    end

    it 'should read heterogeneous clustering orders' do
      expect(table.clustering_columns.map(&:clustering_order)).to eq([:desc, :asc])
    end
  end # describe 'reading compound non-partition key'

  describe 'reading compound partition key' do
    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (
          blog_subdomain text,
          permalink ascii,
          PRIMARY KEY ((blog_subdomain, permalink))
        )
      CQL
    end

    it 'should read partition key names' do
      expect(table.partition_key_columns.map(&:name)).to eq([:blog_subdomain, :permalink])
    end

    it 'should read partition key types' do
      expect(table.partition_key_columns.map(&:type)).
        to eq([Cequel::Type::Text.instance, Cequel::Type::Ascii.instance])
    end

    it 'should have empty nonpartition keys' do
      expect(table.clustering_columns).to be_empty
    end

  end # describe 'reading compound partition key'

  describe 'reading compound partition and non-partition keys' do
    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (
          blog_subdomain text,
          permalink ascii,
          author_id uuid,
          published_at timestamp,
          PRIMARY KEY ((blog_subdomain, permalink), author_id, published_at)
        )
        WITH CLUSTERING ORDER BY (author_id ASC, published_at DESC)
      CQL
    end

    it 'should read partition key names' do
      expect(table.partition_key_columns.map(&:name)).to eq([:blog_subdomain, :permalink])
    end

    it 'should read partition key types' do
      expect(table.partition_key_columns.map(&:type)).
        to eq([Cequel::Type::Text.instance, Cequel::Type::Ascii.instance])
    end

    it 'should read non-partition key names' do
      expect(table.clustering_columns.map(&:name)).
        to eq([:author_id, :published_at])
    end

    it 'should read non-partition key types' do
      expect(table.clustering_columns.map(&:type)).to eq(
        [Cequel::Type::Uuid.instance, Cequel::Type::Timestamp.instance]
      )
    end

    it 'should read clustering order' do
      expect(table.clustering_columns.map(&:clustering_order)).to eq([:asc, :desc])
    end

  end # describe 'reading compound partition and non-partition keys'

  describe 'reading data columns' do

    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (
          blog_subdomain text,
          permalink ascii,
          title text,
          author_id uuid,
          categories LIST <text>,
          tags SET <text>,
          trackbacks MAP <timestamp,ascii>,
          PRIMARY KEY (blog_subdomain, permalink)
        )
      CQL
      cequel.execute('CREATE INDEX posts_author_id_idx ON posts (author_id)')
    end

    it 'should read types of scalar data columns' do
      expect(table.data_columns.find { |column| column.name == :title }.type).
        to eq(Cequel::Type[:text])
      expect(table.data_columns.find { |column| column.name == :author_id }.type).
        to eq(Cequel::Type[:uuid])
    end

    it 'should read index attributes' do
      expect(table.data_columns.find { |column| column.name == :author_id }.index_name).
        to eq(:posts_author_id_idx)
    end

    it 'should leave nil index for non-indexed columns' do
      expect(table.data_columns.find { |column| column.name == :title }.index_name).
        to be_nil
    end

    it 'should read list columns' do
      expect(table.data_columns.find { |column| column.name == :categories }).
        to be_a(Cequel::Schema::List)
    end

    it 'should read list column type' do
      expect(table.data_columns.find { |column| column.name == :categories }.type).
        to eq(Cequel::Type[:text])
    end

    it 'should read set columns' do
      expect(table.data_columns.find { |column| column.name == :tags }).
        to be_a(Cequel::Schema::Set)
    end

    it 'should read set column type' do
      expect(table.data_columns.find { |column| column.name == :tags }.type).
        to eq(Cequel::Type[:text])
    end

    it 'should read map columns' do
      expect(table.data_columns.find { |column| column.name == :trackbacks }).
        to be_a(Cequel::Schema::Map)
    end

    it 'should read map column key type' do
      expect(table.data_columns.find { |column| column.name == :trackbacks }.key_type).
        to eq(Cequel::Type[:timestamp])
    end

    it 'should read map column value type' do
      expect(table.data_columns.find { |column| column.name == :trackbacks }.
        value_type).to eq(Cequel::Type[:ascii])
    end

  end # describe 'reading data columns'

  describe 'reading storage properties' do

    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (permalink text PRIMARY KEY)
        WITH bloom_filter_fp_chance = 0.02
        AND comment = 'Posts table'
        AND compaction = {
          'class' : 'SizeTieredCompactionStrategy',
          'bucket_high' : 1.8,
          'max_threshold' : 64,
          'min_sstable_size' : 50,
          'tombstone_compaction_interval' : 2
        } AND compression = {
          'sstable_compression' : 'DeflateCompressor',
          'chunk_length_kb' : 128,
          'crc_check_chance' : 0.5
        }
      CQL
    end

    it 'should read float properties' do
      expect(table.property(:bloom_filter_fp_chance)).to eq(0.02)
    end

    it 'should read string properties' do
      expect(table.property(:comment)).to eq('Posts table')
    end

    it 'should read and simplify compaction class' do
      expect(table.property(:compaction)[:class]).
        to eq('SizeTieredCompactionStrategy')
    end

    it 'should read float properties from compaction hash' do
      expect(table.property(:compaction)[:bucket_high]).to eq(1.8)
    end

    it 'should read integer properties from compaction hash' do
      expect(table.property(:compaction)[:max_threshold]).to eq(64)
    end

    it 'should read and simplify compression class' do
      expect(table.property(:compression)[:sstable_compression]).
        to eq('DeflateCompressor')
    end

    it 'should read integer properties from compression class' do
      expect(table.property(:compression)[:chunk_length_kb]).to eq(128)
    end

    it 'should read float properties from compression class' do
      expect(table.property(:compression)[:crc_check_chance]).to eq(0.5)
    end

    it 'should recognize no compact storage' do
      expect(table).not_to be_compact_storage
    end

  end # describe 'reading storage properties'

  describe 'skinny-row compact storage' do
    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (permalink text PRIMARY KEY, title text, body text)
        WITH COMPACT STORAGE
      CQL
    end
    subject { table }

    it { is_expected.to be_compact_storage }
    its(:partition_key_columns) { should ==
      [Cequel::Schema::PartitionKey.new(:permalink, :text)] }
    its(:clustering_columns) { should be_empty }
    specify { expect(table.data_columns).to contain_exactly(
      Cequel::Schema::DataColumn.new(:title, :text),
      Cequel::Schema::DataColumn.new(:body, :text)) }
  end

  describe 'wide-row compact storage' do
    before do
      cequel.execute <<-CQL
        CREATE TABLE posts (
          blog_subdomain text,
          id uuid,
          data text,
          PRIMARY KEY (blog_subdomain, id)
        )
        WITH COMPACT STORAGE
      CQL
    end
    subject { table }

    it { is_expected.to be_compact_storage }
    its(:partition_key_columns) { should ==
      [Cequel::Schema::PartitionKey.new(:blog_subdomain, :text)] }
    its(:clustering_columns) { should ==
      [Cequel::Schema::ClusteringColumn.new(:id, :uuid)] }
    its(:data_columns) { should ==
      [Cequel::Schema::DataColumn.new(:data, :text)] }
  end

  describe 'skinny-row legacy table', thrift: true do
    before do
      legacy_connection.execute <<-CQL
        CREATE TABLE posts (permalink text PRIMARY KEY, title text, body text)
      CQL
    end
    subject { table }

    it { is_expected.to be_compact_storage }
    its(:partition_key_columns) { is_expected.to eq(
      [Cequel::Schema::PartitionKey.new(:permalink, :text)]
    ) }
    its(:clustering_columns) { is_expected.to be_empty }
    its(:data_columns) { is_expected.to match_array(
      [Cequel::Schema::DataColumn.new(:title, :text),
        Cequel::Schema::DataColumn.new(:body, :text)]
    ) }
  end

  describe 'wide-row legacy table', thrift: true do
    before do
      legacy_connection.execute(<<-CQL2)
        CREATE COLUMNFAMILY posts (blog_subdomain text PRIMARY KEY)
        WITH comparator=uuid AND default_validation=text
      CQL2
    end
    subject { table }

    it { is_expected.to be_compact_storage }
    its(:partition_key_columns) { is_expected.to eq(
      [Cequel::Schema::PartitionKey.new(:blog_subdomain, :text)]
    ) }
    its(:clustering_columns) { is_expected.to eq(
      [Cequel::Schema::ClusteringColumn.new(:column1, :uuid)]
    ) }
    its(:data_columns) { is_expected.to eq(
      [Cequel::Schema::DataColumn.new(:value, :text)]
    ) }
  end
end
