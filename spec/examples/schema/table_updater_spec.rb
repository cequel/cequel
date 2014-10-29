# -*- encoding : utf-8 -*-
require File.expand_path('../../spec_helper', __FILE__)

describe Cequel::Schema::TableUpdater do
  before do
    cequel.schema.create_table(:posts) do
      key :blog_subdomain, :text
      key :permalink, :text
      column :title, :ascii
      column :body, :text
    end
  end

  after { cequel.schema.drop_table(:posts) }

  let(:table) { cequel.schema.read_table(:posts) }

  describe '#add_column' do
    before do
      cequel.schema.alter_table(:posts) do
        add_column :published_at, :timestamp
      end
    end

    it 'should add the column with the given type' do
      expect(table.data_column(:published_at).type).to eq(Cequel::Type[:timestamp])
    end
  end

  describe '#add_list' do
    before do
      cequel.schema.alter_table(:posts) do
        add_list :author_names, :text
      end
    end

    it 'should add the list' do
      expect(table.data_column(:author_names)).to be_a(Cequel::Schema::List)
    end

    it 'should set the given type' do
      expect(table.data_column(:author_names).type).to eq(Cequel::Type[:text])
    end
  end

  describe '#add_set' do
    before do
      cequel.schema.alter_table(:posts) do
        add_set :author_names, :text
      end
    end

    it 'should add the list' do
      expect(table.data_column(:author_names)).to be_a(Cequel::Schema::Set)
    end

    it 'should set the given type' do
      expect(table.data_column(:author_names).type).to eq(Cequel::Type[:text])
    end
  end

  describe '#add_map' do
    before do
      cequel.schema.alter_table(:posts) do
        add_map :trackbacks, :timestamp, :ascii
      end
    end

    it 'should add the list' do
      expect(table.data_column(:trackbacks)).to be_a(Cequel::Schema::Map)
    end

    it 'should set the key type' do
      expect(table.data_column(:trackbacks).key_type).
        to eq(Cequel::Type[:timestamp])
    end

    it 'should set the value type' do
      expect(table.data_column(:trackbacks).value_type).
        to eq(Cequel::Type[:ascii])
    end
  end

  describe '#change_column' do
    before do
      cequel.schema.alter_table(:posts) do
        change_column :title, :text
      end
    end

    it 'should change the type' do
      expect(table.data_column(:title).type).to eq(Cequel::Type[:text])
    end
  end

  describe '#rename_column' do
    before do
      cequel.schema.alter_table(:posts) do
        rename_column :permalink, :slug
      end
    end

    it 'should change the name' do
      expect(table.clustering_column(:slug)).to be
      expect(table.clustering_column(:permalink)).to be_nil
    end
  end

  describe '#change_properties' do
    before do
      cequel.schema.alter_table(:posts) do
        change_properties :comment => 'Test Comment'
      end
    end

    it 'should change properties' do
      expect(table.properties[:comment].value).to eq('Test Comment')
    end
  end

  describe '#add_index' do
    before do
      cequel.schema.alter_table(:posts) do
        create_index :title
      end
    end

    it 'should add the index' do
      expect(table.data_column(:title)).to be_indexed
    end
  end

  describe '#drop_index' do
    before do
      cequel.schema.alter_table(:posts) do
        create_index :title
        drop_index :posts_title_idx
      end
    end

    it 'should drop the index' do
      expect(table.data_column(:title)).not_to be_indexed
    end
  end

  describe '#drop_column' do
    before do
      pending 'Support in a future Cassandra version'
      cequel.schema.alter_table(:posts) do
        drop_column :body
      end
    end

    it 'should remove the column' do
      expect(table.data_column(:body)).to be_nil
    end
  end
end
