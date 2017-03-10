# -*- encoding : utf-8 -*-
require File.expand_path('../../spec_helper', __FILE__)

module Cequel::Schema
  describe TableDiffer do

    let(:table_name) { |ex| unique_table_name("posts", ex) }
    let(:orig_table) {
      Table.new(table_name).tap do |t|
        t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
        t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text])
        t.add_column DataColumn.new(:body, Cequel::Type[:text])
        t.add_column DataColumn.new(:author_name, Cequel::Type[:text], :author_name_idx)
        t.add_property TableProperty.build(:comment, "Orig comment")
      end
    }

    describe ".new" do
      it "returns a TableDiffer" do
        expect(
          described_class.new(orig_table, orig_table)
        ).to be_kind_of described_class
      end
    end

    describe ".call" do
      it "returns a Patch" do
        updated_table = Table.new(table_name).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text])
          t.add_column DataColumn.new(:body, Cequel::Type[:text])
          t.add_column DataColumn.new(:author_name, Cequel::Type[:text], :author_name_idx)
          t.add_property TableProperty.build(:comment, "Orig comment")
        end

        expect(
          described_class.new(orig_table, updated_table).call
        ).to be_kind_of Patch
      end

      it "fails for table name changes" do
        renamed_table = Table.new(:fancy_new_name).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text])
          t.add_column DataColumn.new(:body, Cequel::Type[:text])
          t.add_column DataColumn.new(:author_name, Cequel::Type[:text], :author_name_idx)
          t.add_property TableProperty.build(:comment, "Orig comment")
        end

        expect{
          described_class.new(orig_table, renamed_table).call
        }.to raise_error(Cequel::InvalidSchemaMigration)
      end

      it "succeed if name difference it immaterial" do
        equiv_table = Table.new(table_name.to_s).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text])
          t.add_column DataColumn.new(:body, Cequel::Type[:text])
          t.add_column DataColumn.new(:author_name, Cequel::Type[:text], :author_name_idx)
          t.add_property TableProperty.build(:comment, "Orig comment")
        end

        expect{
          described_class.new(orig_table, equiv_table).call
        }.not_to raise_error
      end

      it "fails for type changes" do
        updated_table = Table.new(table_name).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text])
          t.add_column DataColumn.new(:body, Cequel::Type[:ascii])
        end

        expect{
          described_class.new(orig_table, updated_table).call
        }.to raise_error(Cequel::InvalidSchemaMigration)
      end

      it "fails for partition key changes" do
        updated_table = Table.new(table_name).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column PartitionKey.new(:date, Cequel::Type[:timestamp])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text])
          t.add_column DataColumn.new(:body, Cequel::Type[:text])
          t.add_column DataColumn.new(:author_name, Cequel::Type[:text], :author_name_idx)
          t.add_property TableProperty.build(:comment, "Orig comment")
        end

        expect{
          described_class.new(orig_table, updated_table).call
        }.to raise_error(Cequel::InvalidSchemaMigration)
      end

      it "fails for clustering order changes" do
        updated_table = Table.new(table_name).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text], :desc)
          t.add_column DataColumn.new(:body, Cequel::Type[:text])
          t.add_column DataColumn.new(:author_name, Cequel::Type[:text], :author_name_idx)
          t.add_property TableProperty.build(:comment, "Orig comment")
        end

        expect{
          described_class.new(orig_table, updated_table).call
        }.to raise_error(Cequel::InvalidSchemaMigration)
      end

      it "ignore immaterial changes to clustering order" do
        unchanged_table = Table.new(table_name).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text], :asc)
          t.add_column DataColumn.new(:body, Cequel::Type[:text])
          t.add_column DataColumn.new(:author_name, Cequel::Type[:text], :author_name_idx)
          t.add_property TableProperty.build(:comment, "Orig comment")
        end

        expect{
          described_class.new(orig_table, unchanged_table).call
        }.not_to raise_error
      end

      it "detects a lack of changes changes" do
        unchanged_table = Table.new(table_name).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text])
          t.add_column DataColumn.new(:body, Cequel::Type[:text])
          t.add_column DataColumn.new(:author_name, Cequel::Type[:text], :author_name_idx)
          t.add_property TableProperty.build(:comment, "Orig comment")
        end

        expect(
          described_class.new(orig_table, unchanged_table).call
        ).to be_empty
      end

      it "detects new columns" do
        updated_table = Table.new(table_name).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text])
          t.add_column DataColumn.new(:body, Cequel::Type[:text])
          t.add_column DataColumn.new(:created_at, Cequel::Type[:timestamp])
        end

        expect(
          described_class.new(orig_table, updated_table).call
        ).to add_column(:created_at).of_type(:timestamp)
      end

      it "detects added index to existing column" do
        updated_table = Table.new(table_name).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text])
          t.add_column DataColumn.new(:body, Cequel::Type[:text], :my_index_name)
        end

        expect(
          described_class.new(orig_table, updated_table).call
        ).to add_index(:my_index_name).of_column(:body)
      end

      it "detects dropped index" do
        updated_table = Table.new(table_name).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text])
          t.add_column DataColumn.new(:body, Cequel::Type[:text])
          t.add_column DataColumn.new(:author_name, Cequel::Type[:text])
        end

        expect(
          described_class.new(orig_table, updated_table).call
        ).to drop_index(:author_name_idx)
      end

      it "detects added index to new column" do
        updated_table = Table.new(table_name).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text])
          t.add_column DataColumn.new(:category, Cequel::Type[:text], :posts_category_idx)
        end

        expect(
          described_class.new(orig_table, updated_table).call
        ).to add_index(:posts_category_idx).of_column(:category)
      end

      it "ignores dropped columns" do
        updated_table = Table.new(table_name).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text])
          t.add_column DataColumn.new(:body, Cequel::Type[:text])
          t.add_property TableProperty.build(:comment, "Orig comment")
        end

        expect(
          described_class.new(orig_table, updated_table).call
        ).to be_empty
      end

      it "detects new property" do
        updated_table = Table.new(table_name).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text])
          t.add_property TableProperty.build(:comment, "test")
        end

        expect(
          described_class.new(orig_table, updated_table).call
        ).to set_property(:comment, "test")
      end
    end

    # Background

    matcher :set_property do |expected_name, expected_val|
      match do |actual_patch|
        actual_patch.changes
          .any? { |actual_change|
          actual_change.is_a?(Cequel::Schema::Patch::SetTableProperties) &&
            actual_change.properties
            .any?{|p|
            p.name.to_s == expected_name.to_s &&
              expected_val === p.value
          }
        }
      end
    end

    matcher :drop_index do |index_name|
      match do |actual_patch|
        actual_patch.changes
          .any? { |actual_change|
          actual_change.is_a?(Cequel::Schema::Patch::DropIndex) &&
            actual_change.index_name.to_s == index_name.to_s
        }
      end
    end

    matcher :add_index do |index_name|
      @column_matcher = ->(_){true}

      match do |actual_patch|
        actual_patch.changes
          .any? { |actual_change|
          actual_change.is_a?(Cequel::Schema::Patch::AddIndex) &&
            @column_matcher === actual_change.column &&
            actual_change.index_name.to_s == index_name.to_s
        }
      end

      chain :of_column do |col_name|
        @column_matcher = ->(col) { col.name.to_s == col_name.to_s }
      end
    end

    matcher :add_column do |name|
      @type_matcher = ->(_){true}

      match do |actual_patch|
        actual_patch.changes
          .any? { |actual_change|
          actual_change.is_a?(Cequel::Schema::Patch::AddColumn) &&
            actual_change.column.name == name &&
            @type_matcher === actual_change.column.type
        }
      end

      chain :of_type do |type_or_matcher|
        @type_matcher = if type_or_matcher.respond_to?(:call)
                          type_or_matcher
                        elsif t = Cequel::Type[type_or_matcher]
                          ->(type) { type == t }
                        else
                          fail "unsupported type matcher: #{type_or_matcher.inspect}"
                        end
      end
    end
  end
end
