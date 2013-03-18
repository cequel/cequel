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
      table.column(:title).type.should == Cequel::Type[:text] #etc.
    end
  end

  context 'with an existing table' do
    before do
      cequel.schema.create_table :posts do
        key :blog_subdomain, :text
        key :permalink, :text
        column :title, :text, :index => true
        column :body, :text
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
          column :title, :text
          column :body, :ascii
          column :primary_author_id, :uuid, :index => true
          column :created_at, :timestamp, :index => true
          column :published_at, :timestamp
          set :author_names, :text
          list :categories, :text
          with :comment, 'Test Table 2.0'
        end
      end

      it 'should rename keys' do
        table.clustering_columns.first.name.should == :post_permalink
      end

      it 'should add new columns' do
        table.column(:published_at).type.should == Cequel::Type[:timestamp]
      end

      it 'should add new collections' do
        table.column(:categories).should be_a(Cequel::Schema::List)
      end

      it 'should add new column with index' do
        table.column(:primary_author_id).should be_indexed
      end

      it 'should add index to existing columns' do
        table.column(:created_at).should be_indexed
      end

      it 'should drop index from existing columns' do
        table.column(:title).should_not be_indexed
      end

      it 'should change column type' do
        table.column(:body).type.should == Cequel::Type[:ascii]
      end

      it 'should change properties' do
        table.property(:comment).should == 'Test Table 2.0'
      end

    end

    context 'invalid migrations' do

      it 'should not allow changing type of key' do
        expect {
          cequel.schema.sync_table :posts do
            key :blog_subdomain, :text
            key :permalink, :ascii
            column :title, :text
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
            column :title, :text
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
            column :title, :text
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
            column :title, :text
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
            column :title, :text
            column :body, :text
            column :created_at, :timestamp
            list :author_names, :text
            with :comment, 'Test Table'
          end
        }.to raise_error(Cequel::InvalidSchemaMigration)
      end

      it 'should not allow changing of clustering order'

    end


  end

end
