# -*- encoding : utf-8 -*-
require File.expand_path('../../spec_helper', __FILE__)

describe Cequel::Schema::TableWriter do

  let(:table) { cequel.schema.read_table(:posts) }

  describe '#create_table' do

    after do
      cequel.schema.drop_table(:posts)
    end

    describe 'with simple skinny table' do
      before do
        cequel.schema.create_table(:posts) do
          key :permalink, :ascii
          column :title, :text
        end
      end

      it 'should create key alias' do
        expect(table.partition_key_columns.map(&:name)).to eq([:permalink])
      end

      it 'should set key validator' do
        expect(table.partition_key_columns.map(&:type)).to eq([Cequel::Type[:ascii]])
      end

      it 'should set non-key columns' do
        expect(table.columns.find { |column| column.name == :title }.type).
          to eq(Cequel::Type[:text])
      end
    end

    describe 'with multi-column primary key' do
      before do
        cequel.schema.create_table(:posts) do
          key :blog_subdomain, :ascii
          key :permalink, :ascii
          column :title, :text
        end
      end

      it 'should create key alias' do
        expect(table.partition_key_columns.map(&:name)).to eq([:blog_subdomain])
      end

      it 'should set key validator' do
        expect(table.partition_key_columns.map(&:type)).to eq([Cequel::Type[:ascii]])
      end

      it 'should create non-partition key components' do
        expect(table.clustering_columns.map(&:name)).to eq([:permalink])
      end

      it 'should set type for non-partition key components' do
        expect(table.clustering_columns.map(&:type)).to eq([Cequel::Type[:ascii]])
      end
    end

    describe 'with composite partition key' do
      before do
        cequel.schema.create_table(:posts) do
          partition_key :blog_subdomain, :ascii
          partition_key :permalink, :ascii
          column :title, :text
        end
      end

      it 'should create all partition key components' do
        expect(table.partition_key_columns.map(&:name)).to eq([:blog_subdomain, :permalink])
      end

      it 'should set key validators' do
        expect(table.partition_key_columns.map(&:type)).
          to eq([Cequel::Type[:ascii], Cequel::Type[:ascii]])
      end
    end

    describe 'with composite partition key and non-partition keys' do
      before do
        cequel.schema.create_table(:posts) do
          partition_key :blog_subdomain, :ascii
          partition_key :permalink, :ascii
          key :month, :timestamp
          column :title, :text
        end
      end

      it 'should create all partition key components' do
        expect(table.partition_key_columns.map(&:name)).
          to eq([:blog_subdomain, :permalink])
      end

      it 'should set key validators' do
        expect(table.partition_key_columns.map(&:type)).
          to eq([Cequel::Type[:ascii], Cequel::Type[:ascii]])
      end

      it 'should create non-partition key components' do
        expect(table.clustering_columns.map(&:name)).to eq([:month])
      end

      it 'should set type for non-partition key components' do
        expect(table.clustering_columns.map(&:type)).to eq([Cequel::Type[:timestamp]])
      end
    end

    describe 'collection types' do
      before do
        cequel.schema.create_table(:posts) do
          key :permalink, :ascii
          column :title, :text
          list :authors, :blob
          set :tags, :text
          map :trackbacks, :timestamp, :ascii
        end
      end

      it 'should create list' do
        expect(table.data_column(:authors)).to be_a(Cequel::Schema::List)
      end

      it 'should set correct type for list' do
        expect(table.data_column(:authors).type).to eq(Cequel::Type[:blob])
      end

      it 'should create set' do
        expect(table.data_column(:tags)).to be_a(Cequel::Schema::Set)
      end

      it 'should set correct type for set' do
        expect(table.data_column(:tags).type).to eq(Cequel::Type[:text])
      end

      it 'should create map' do
        expect(table.data_column(:trackbacks)).to be_a(Cequel::Schema::Map)
      end

      it 'should set correct key type' do
        expect(table.data_column(:trackbacks).key_type).
          to eq(Cequel::Type[:timestamp])
      end

      it 'should set correct value type' do
        expect(table.data_column(:trackbacks).value_type).
          to eq(Cequel::Type[:ascii])
      end
    end

    describe 'storage properties' do
      before do
        cequel.schema.create_table(:posts) do
          key :permalink, :ascii
          column :title, :text
          with :comment, 'Blog posts'
          with :compression,
            :sstable_compression => "DeflateCompressor",
            :chunk_length_kb => 64
        end
      end

      it 'should set simple properties' do
        expect(table.property(:comment)).to eq('Blog posts')
      end

      it 'should set map collection properties' do
        expect(table.property(:compression)).to eq({
          :sstable_compression => 'DeflateCompressor',
          :chunk_length_kb => 64
        })
      end
    end

    describe 'compact storage' do
      before do
        cequel.schema.create_table(:posts) do
          key :permalink, :ascii
          column :title, :text
          compact_storage
        end
      end

      it 'should have compact storage' do
        expect(table).to be_compact_storage
      end
    end

    describe 'clustering order' do
      before do
        cequel.schema.create_table(:posts) do
          key :blog_permalink, :ascii
          key :id, :uuid, :desc
          column :title, :text
        end
      end

      it 'should set clustering order' do
        expect(table.clustering_columns.map(&:clustering_order)).to eq([:desc])
      end
    end

    describe 'indices' do
      it 'should create indices' do
        cequel.schema.create_table(:posts) do
          key :blog_permalink, :ascii
          key :id, :uuid, :desc
          column :title, :text, :index => true
        end
        expect(table.data_column(:title)).to be_indexed
      end

      it 'should create indices with specified name' do
        cequel.schema.create_table(:posts) do
          key :blog_permalink, :ascii
          key :id, :uuid, :desc
          column :title, :text, :index => :silly_idx
        end
        expect(table.data_column(:title).index_name).to eq(:silly_idx)
      end
    end

  end

end
