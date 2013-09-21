require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record::Schema do
  after { cequel.schema.drop_table(:posts) }
  subject { cequel.schema.read_table(:posts) }

  let(:model) do
    Class.new(Cequel::Record::Base) do
      self.table_name = 'posts'

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
    specify { subject.property(:comment).should == 'Blog Posts' }
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
