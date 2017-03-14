# -*- encoding : utf-8 -*-
require File.expand_path('../../spec_helper', __FILE__)

describe Cequel::Schema::TableReader do
  let(:table_name) { :"posts_#{SecureRandom.hex(4)}" }

  after do
    cequel.schema.drop_table(table_name)
  end

  describe ".read(keyspace, table_name)" do
    before do
      cequel.execute("CREATE TABLE #{table_name} (permalink text PRIMARY KEY)")
      cequel.send(:cluster).refresh_schema
    end

    it "returns a table" do
      expect(
        described_class.read(cequel, table_name)
      ).to be_kind_of Cequel::Schema::Table
    end
  end

  describe "#call" do
    let(:table) { described_class.new(fetch_table_data).call }

    context 'simple key' do
      before do
        cequel.execute("CREATE TABLE #{table_name} (permalink text PRIMARY KEY)")
        cequel.send(:cluster).refresh_schema
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
    end # context 'simple key'

    context 'single cluster key' do
      before do
        cequel.execute <<-CQL
        CREATE TABLE #{table_name} (
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
    end # context 'single cluster key'

    context 'reverse-ordered cluster key' do
      before do
        cequel.execute <<-CQL
        CREATE TABLE #{table_name} (
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
    end # context 'reverse-ordered cluster key'

    context 'compound cluster key' do
      before do
        cequel.execute <<-CQL
        CREATE TABLE #{table_name} (
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
    end # context 'compound context key'

    context 'compound partition key' do
      before do
        cequel.execute <<-CQL
        CREATE TABLE #{table_name} (
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

    end # context 'compound partition key'

    context 'compound partition and cluster keys' do
      before do
        cequel.execute <<-CQL
        CREATE TABLE #{table_name} (
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

    end # context 'compound partition and context keys'

    context 'data columns' do

      before do
        cequel.execute <<-CQL
          CREATE TABLE #{table_name} (
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
        cequel.execute("CREATE INDEX posts_author_id_idx ON #{table_name} (author_id)")
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

    end # context 'data columns'

    context 'storage properties' do

      before do
        cequel.execute <<-CQL
          CREATE TABLE #{table_name} (permalink text PRIMARY KEY)
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
        expect(table.property(:compression)[:sstable_compression] ||
               table.property(:compression)[:class]).
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
    end # context 'storage properties'

    context 'skinny-row compact storage' do
      before do
        cequel.execute <<-CQL
          CREATE TABLE #{table_name} (permalink text PRIMARY KEY, title text, body text)
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

    context 'wide-row compact storage' do
      before do
        cequel.execute <<-CQL
          CREATE TABLE #{table_name} (
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

    context 'materialized view exists', cql: '~> 3.4' do
      let!(:name) { table_name }
      let(:view_name) { "#{name}_view" }
      before do
        cequel.execute <<-CQL
          CREATE TABLE #{table_name} (
            blog_subdomain text,
            permalink ascii,
            PRIMARY KEY (blog_subdomain, permalink)
          )
        CQL
        cequel.execute <<-CQL
          CREATE MATERIALIZED VIEW #{view_name} AS
            SELECT blog_subdomain, permalink
            FROM #{name}
            WHERE blog_subdomain IS NOT NULL AND permalink IS NOT NULL
            PRIMARY KEY ( blog_subdomain, permalink )
        CQL
      end
      after do
        cequel.schema.drop_materialized_view(view_name)
      end

      let(:view) { described_class.new(fetch_view_data).call }

      it "recognizes that regular tables are not views" do
        expect( table.materialized_view? ).to be false
      end

      it "recognizes thats view tables are views" do
        expect( view.materialized_view? ).to be true
      end
    end

    context 'skinny-row legacy table', thrift: true do
      before do
        legacy_connection.execute <<-CQL
          CREATE TABLE #{table_name} (permalink text PRIMARY KEY, title text, body text)
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

    context 'wide-row legacy table', thrift: true do
      before do
        legacy_connection.execute(<<-CQL2)
          CREATE COLUMNFAMILY #{table_name} (blog_subdomain text PRIMARY KEY)
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

  def fetch_table_data(name=table_name)
    cequel.send(:cluster).refresh_schema
    cequel.send(:cluster)
      .keyspace(cequel.name.to_s)
      .table(name.to_s)
  end

  def fetch_view_data(name=view_name)
    cequel.send(:cluster).refresh_schema
    cequel.send(:cluster)
      .keyspace(cequel.name.to_s)
      .materialized_view(name.to_s)
  end
end
