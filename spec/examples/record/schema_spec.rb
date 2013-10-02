require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record::Schema do
  context 'CQL3 table' do
    after { cequel.schema.drop_table(:posts) }
    subject { cequel.schema.read_table(:posts) }

    let(:model) do
      Class.new do
        include Cequel::Record
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

  context 'wide-row legacy table' do
    let(:legacy_model) do
      Class.new do
        include Cequel::Record
        self.table_name = 'legacy_posts'

        key :blog_subdomain, :text
        key :id, :uuid
        column :data, :text

        compact_storage
      end
    end
    after { cequel.schema.drop_table(:legacy_posts) }
    subject { cequel.schema.read_table(:legacy_posts) }

    context 'new model' do
      before { legacy_model.synchronize_schema }

      its(:partition_key_columns) { should == [Cequel::Schema::Column.new(:blog_subdomain, :text)] }
      its(:clustering_columns) { should == [Cequel::Schema::Column.new(:id, :uuid)] }
      it { should be_compact_storage }
      its(:data_columns) { should == [Cequel::Schema::Column.new(:data, :text)] }
    end

    context 'existing model' do
      before do
        legacy_connection.execute(<<-CQL2)
          CREATE COLUMNFAMILY legacy_posts (blog_subdomain text PRIMARY KEY)
          WITH comparator=uuid AND default_validation=text
        CQL2
        legacy_model.synchronize_schema
      end

      its(:partition_key_columns) { should == [Cequel::Schema::Column.new(:blog_subdomain, :text)] }
      its(:clustering_columns) { should == [Cequel::Schema::Column.new(:id, :uuid)] }
      it { should be_compact_storage }
      its(:data_columns) { should == [Cequel::Schema::Column.new(:data, :text)] }

      it 'should be able to synchronize schema again' do
        expect { legacy_model.synchronize_schema }.to_not raise_error
      end
    end
  end
end
