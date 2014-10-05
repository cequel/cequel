# -*- encoding : utf-8 -*-
require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record::Schema do
  context 'CQL3 table' do
    after { cequel.schema.drop_table(table_name) }
    subject { cequel.schema.read_table(table_name) }

    let(:table_name) { 'posts_' + SecureRandom.hex(4) }

    let(:model) do
      model_table_name = table_name
      Class.new do
        include Cequel::Record
        self.table_name = model_table_name

        key :permalink, :text
        column :title, :text
        list :categories, :text
        set :tags, :text
        map :trackbacks, :timestamp, :text
        table_property :comment, 'Blog Posts'
      end
    end

    context 'new model with simple primary key' do
      before { model.synchronize_schema }

      its(:partition_key_columns) { should == [Cequel::Schema::Column.new(:permalink, :text)] }
      its(:data_columns) { should include(Cequel::Schema::Column.new(:title, :text)) }
      its(:data_columns) { should include(Cequel::Schema::List.new(:categories, :text)) }
      its(:data_columns) { should include(Cequel::Schema::Set.new(:tags, :text)) }
      its(:data_columns) { should include(Cequel::Schema::Map.new(:trackbacks, :timestamp, :text)) }
      specify { expect(subject.property(:comment)).to eq('Blog Posts') }
    end

    context 'existing model with additional attribute' do
      before do
        cequel.schema.create_table :posts do
          key :permalink, :text
          column :title, :text
          list :categories, :text
          set :tags, :text
        end
        model.synchronize_schema
      end

      its(:data_columns) { should include(Cequel::Schema::Map.new(:trackbacks, :timestamp, :text)) }
    end
  end

  context 'CQL3 table with reversed clustering column' do
    let(:table_name) { 'posts_' + SecureRandom.hex(4) }

    let(:model) do
      model_table_name = table_name
      Class.new do
        include Cequel::Record
        self.table_name = model_table_name

        key :blog_id, :uuid
        key :id, :timeuuid, order: :desc
        column :title, :text
      end
    end

    before { model.synchronize_schema }
    after { cequel.schema.drop_table(table_name) }
    subject { cequel.schema.read_table(table_name) }

    it 'should order clustering column descending' do
      expect(subject.clustering_columns.first.clustering_order).to eq(:desc)
    end
  end

  context 'wide-row legacy table' do
    let(:table_name) { 'legacy_posts_' + SecureRandom.hex(4) }

    let(:legacy_model) do
      model_table_name = table_name
      Class.new do
        include Cequel::Record
        self.table_name = model_table_name

        key :blog_subdomain, :text
        key :id, :uuid
        column :data, :text

        compact_storage
      end
    end
    after { cequel.schema.drop_table(table_name) }
    subject { cequel.schema.read_table(table_name) }

    context 'new model' do
      before { legacy_model.synchronize_schema }

      its(:partition_key_columns) { should == [Cequel::Schema::Column.new(:blog_subdomain, :text)] }
      its(:clustering_columns) { should == [Cequel::Schema::Column.new(:id, :uuid)] }
      it { is_expected.to be_compact_storage }
      its(:data_columns) { should == [Cequel::Schema::Column.new(:data, :text)] }
    end

    context 'existing model', thrift: true do
      before do
        legacy_connection.execute(<<-CQL2)
          CREATE COLUMNFAMILY #{table_name} (blog_subdomain text PRIMARY KEY)
          WITH comparator=uuid AND default_validation=text
        CQL2
        legacy_model.synchronize_schema
      end

      its(:partition_key_columns) { is_expected.to eq([Cequel::Schema::Column.new(:blog_subdomain, :text)]) }
      its(:clustering_columns) { is_expected.to eq([Cequel::Schema::Column.new(:id, :uuid)]) }
      it { is_expected.to be_compact_storage }
      its(:data_columns) { is_expected.to eq([Cequel::Schema::Column.new(:data, :text)]) }

      it 'should be able to synchronize schema again' do
        expect { legacy_model.synchronize_schema }.to_not raise_error
      end
    end
  end
end
