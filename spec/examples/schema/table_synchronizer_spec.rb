# -*- encoding : utf-8 -*-
require File.expand_path('../../spec_helper', __FILE__)

describe Cequel::Schema::TableSynchronizer do
  let(:table_name) { |ex|
    ent = SecureRandom.hex(4)
    unique_table_name("posts_#{ent}", ex)
  }

  let(:table) { cequel.schema.read_table(table_name) }

  context 'with no existing table' do
    before do
      cequel.schema.sync_table table_name do
        key :blog_subdomain, :text
        key :permalink, :text
        column :title, :text
        column :body, :text
        column :created_at, :timestamp
        set :author_names, :text
        with :comment, 'Test Table'
      end
    end

    after { cequel.schema.drop_table(table_name) }

    it 'should create table' do
      expect(table.column(:title).type).to eq(Cequel::Type[:text]) #etc.
    end
  end

  context 'with an existing table' do
    before do
      cequel.schema.create_table table_name do
        key :blog_subdomain, :text
        key :permalink, :text
        column :title, :ascii, :index => true
        column :body, :ascii
        column :created_at, :timestamp
        set :author_names, :text
        with :comment, 'Test Table'
      end
    end

    after { cequel.schema.drop_table(table_name) }

    it 'should rename cluster keys' do
      cequel.schema.sync_table table_name do
        key :blog_subdomain, :text
        key :post_permalink, :text  # renamed
        column :title, :ascii, :index => true
        column :body, :ascii
        column :created_at, :timestamp
        set :author_names, :text
        with :comment, 'Test Table'
      end

      expect(table.clustering_columns.first.name).to eq(:post_permalink)
    end

    it 'should add new columns' do |ex|
      cequel.schema.sync_table table_name do
        key :blog_subdomain, :text
        key :permalink, :text
        column :title, :ascii, :index => true
        column :body, :ascii
        column :created_at, :timestamp
        set :author_names, :text
        with :comment, 'Test Table - #{ex.description}'

        column :published_at, :timestamp # new
      end

      expect(table.column(:published_at).type).to eq(Cequel::Type[:timestamp])
    end

    it 'should add new collections' do |ex|
      cequel.schema.sync_table table_name do
        key :blog_subdomain, :text
        key :permalink, :text
        column :title, :ascii, :index => true
        column :body, :ascii
        column :created_at, :timestamp
        set :author_names, :text
        with :comment, 'Test Table - #{ex.description}'

        list :categories, :text # new
      end

      expect(table.column(:categories)).to be_a(Cequel::Schema::List)
    end

    it 'should add new column with index' do  |ex|
      cequel.schema.sync_table table_name do
        key :blog_subdomain, :text
        key :permalink, :text
        column :title, :ascii, :index => true
        column :body, :ascii
        column :created_at, :timestamp
        set :author_names, :text
        with :comment, 'Test Table - #{ex.description}'

        column :primary_author_id, :uuid, :index => true # new
      end

      expect(table.column(:primary_author_id)).to be_indexed
    end

    it 'should add index to existing columns' do |ex|
      cequel.schema.sync_table table_name do
        key :blog_subdomain, :text
        key :permalink, :text
        column :title, :ascii, :index => true
        column :body, :ascii
        set :author_names, :text
        with :comment, 'Test Table - #{ex.description}'

        column :created_at, :timestamp, index: true # altered
      end

      expect(table.column(:created_at)).to be_indexed
    end

    it 'should drop index from existing columns' do |ex|
      cequel.schema.sync_table table_name do
        key :blog_subdomain, :text
        key :permalink, :text
        column :title, :ascii  # omitting index: true
        column :body, :ascii
        set :author_names, :text
        with :comment, 'Test Table - #{ex.description}'
      end

      expect(table.column(:title)).not_to be_indexed
    end

    it 'should change properties' do |ex|
      cequel.schema.sync_table table_name do
        key :blog_subdomain, :text
        key :permalink, :text
        column :title, :ascii, :index => true
        column :body, :ascii
        column :created_at, :timestamp
        set :author_names, :text
        with :comment, 'Test Table 2.0' # altered
      end

      expect(table.property(:comment)).to eq('Test Table 2.0')
    end

    context 'invalid migrations' do

      it 'should not allow changing type of key' do
        expect {
          cequel.schema.sync_table table_name do
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

      it 'should change column type' do |ex|
        expect{
          cequel.schema.sync_table table_name do
            key :blog_subdomain, :text
            key :permalink, :text
            column :title, :ascii, :index => true
            column :created_at, :timestamp
            set :author_names, :text
            with :comment, 'Test Table - #{ex.description}'

            column :body, :text # altered
          end
        }.to raise_error(Cequel::InvalidSchemaMigration)
    end

      it 'should not allow adding a key' do
        expect {
          cequel.schema.sync_table table_name do
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
          cequel.schema.sync_table table_name do
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
          cequel.schema.sync_table table_name do
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
          cequel.schema.sync_table table_name do
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
          cequel.schema.sync_table table_name do
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
          cequel.schema.sync_table table_name do
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
