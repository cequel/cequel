# -*- encoding : utf-8 -*-
require File.expand_path('../../spec_helper', __FILE__)

describe Cequel::Schema::TableSynchronizer do

  let(:table) { cequel.schema.read_table(:posts) }

  context 'with no existing table' do
    before do
      cequel.schema.sync_table :posts do
        key :blog_subdomain, :text
        key :permalink, :text
        column :title, :text
        column :body, :text
        column :created_at, :timestamp
        set :author_names, :text
        with :comment, 'Test Table'
      end
    end

    after { cequel.schema.drop_table(:posts) }

    it 'should create table' do
      expect(table.column(:title).type).to eq(Cequel::Type[:text]) #etc.
    end
  end

  context 'with an existing table' do
    before do
      cequel.schema.create_table :posts do
        key :blog_subdomain, :text
        key :permalink, :text
        column :title, :ascii, :index => true
        column :body, :ascii
        column :created_at, :timestamp
        set :author_names, :text
        with :comment, 'Test Table'
      end
    end

    after { cequel.schema.drop_table(:posts) }

    context 'with valid changes' do

      before do
        cequel.schema.sync_table :posts do
          key :blog_subdomain, :text
          key :post_permalink, :text
          column :title, :ascii
          column :body, :text
          column :primary_author_id, :uuid, :index => true
          column :created_at, :timestamp, :index => true
          column :published_at, :timestamp
          set :author_names, :text
          list :categories, :text
          with :comment, 'Test Table 2.0'
        end
      end

      it 'should rename keys' do
        expect(table.clustering_columns.first.name).to eq(:post_permalink)
      end

      it 'should add new columns' do
        expect(table.column(:published_at).type).to eq(Cequel::Type[:timestamp])
      end

      it 'should add new collections' do
        expect(table.column(:categories)).to be_a(Cequel::Schema::List)
      end

      it 'should add new column with index' do
        expect(table.column(:primary_author_id)).to be_indexed
      end

      it 'should add index to existing columns' do
        expect(table.column(:created_at)).to be_indexed
      end

      it 'should drop index from existing columns' do
        expect(table.column(:title)).not_to be_indexed
      end

      it 'should change column type' do
        expect(table.column(:body).type).to eq(Cequel::Type[:text])
      end

      it 'should change properties' do
        expect(table.property(:comment)).to eq('Test Table 2.0')
      end

    end

    context 'invalid migrations' do

      it 'should not allow changing type of key' do
        expect {
          cequel.schema.sync_table :posts do
            key :blog_subdomain, :text
            key :permalink, :ascii
            column :title, :ascii
            column :body, :text
            column :created_at, :timestamp
            set :author_names, :text
            with :comment, 'Test Table'
          end
        }.to raise_error(Cequel::InvalidSchemaMigration)
      end

      it 'should not allow adding a key' do
        expect {
          cequel.schema.sync_table :posts do
            key :blog_subdomain, :text
            key :permalink, :text
            key :year, :int
            column :title, :ascii
            column :body, :text
            column :created_at, :timestamp
            set :author_names, :text
            with :comment, 'Test Table'
          end
        }.to raise_error(Cequel::InvalidSchemaMigration)
      end

      it 'should not allow removing a key' do
        expect {
          cequel.schema.sync_table :posts do
            key :blog_subdomain, :text
            column :title, :ascii
            column :body, :text
            column :created_at, :timestamp
            set :author_names, :text
            with :comment, 'Test Table'
          end
        }.to raise_error(Cequel::InvalidSchemaMigration)
      end

      it 'should not allow changing the partition status of a key' do
        expect {
          cequel.schema.sync_table :posts do
            key :blog_subdomain, :text
            partition_key :permalink, :text
            column :title, :ascii
            column :body, :text
            column :created_at, :timestamp
            set :author_names, :text
            with :comment, 'Test Table'
          end
        }.to raise_error(Cequel::InvalidSchemaMigration)
      end

      it 'should not allow changing the data structure of a column' do
        expect {
          cequel.schema.sync_table :posts do
            key :blog_subdomain, :text
            key :permalink, :text
            column :title, :ascii
            column :body, :text
            column :created_at, :timestamp
            list :author_names, :text
            with :comment, 'Test Table'
          end
        }.to raise_error(Cequel::InvalidSchemaMigration)
      end

      it 'should not allow invalid type transitions of a data column' do
        expect {
          cequel.schema.sync_table :posts do
            key :blog_subdomain, :text
            key :permalink, :text
            column :title, :ascii, :index => true
            column :body, :int
            column :created_at, :timestamp
            set :author_names, :text
            with :comment, 'Test Table'
          end
        }.to raise_error(Cequel::InvalidSchemaMigration)
      end

      it 'should not allow changing clustering order' do
        expect {
          cequel.schema.sync_table :posts do
            key :blog_subdomain, :text
            key :permalink, :text, :desc
            column :title, :ascii, :index => true
            column :body, :ascii
            column :created_at, :timestamp
            set :author_names, :text
            with :comment, 'Test Table'
          end
        }.to raise_error(Cequel::InvalidSchemaMigration)
      end

    end


  end

end
