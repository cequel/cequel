# -*- encoding : utf-8 -*-
require File.expand_path('../../spec_helper', __FILE__)

module Cequel::Schema
  describe Patch do

    let(:table_name) { |ex| unique_table_name("posts", ex) }
    let(:table) {
      Table.new(table_name).tap do |t|
        t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
      end
    }

    describe ".new" do
      it "returns a Patch" do
        expect(
          described_class.new([])
        ).to be_kind_of described_class
      end
    end

    describe "#changes" do
      context "no changes" do
        subject { described_class.new([]) }

        it "returns the changes" do
          expect( subject.changes ).to be_empty
        end
      end

      context "no some changes" do
        let(:change) {
          Patch::AddColumn.new(table, DataColumn.new(:author_name, Cequel::Type[:text]))
        }

        subject { described_class.new([change]) }

        it "returns the changes" do
          expect( subject.changes ).to eq [change]
        end
      end
    end

    describe "#statements" do
      let(:change) {
        Patch::AddColumn.new(table, DataColumn.new(:author_name, Cequel::Type[:text]))
      }

      subject { described_class.new([change]) }

      it "returns a statement for each change" do
        expect( subject.statements.count ).to eq subject.changes.count
        expect( subject.statements.count ).to eq 1
      end
    end
  end

  describe Patch::SetTableProperties do
    let(:table_name) { |ex| unique_table_name("posts", ex) }
    let(:table) {
      Table.new(table_name).tap do |t|
        t.add_property TableProperty.build(:comment, "hello")
      end
    }

    describe "#new" do
      it "returns #{described_class}" do
        expect( described_class.new(table) ).to be_kind_of described_class
      end
    end

    subject { described_class.new(table) }

    describe "#to_cql" do
      it "sets the property" do
        expect( subject.to_cql ).to match /alter +table +"?#{table_name}"? +with/i
        expect( subject.to_cql ).to match /"?comment"? *= *'hello'/i
      end
    end

    describe "#properties" do
      it "returns collection of Table properties to be set" do
        expect( subject.properties ).to all be_kind_of TableProperty
        expect( subject.properties ).to eq table.properties.values
      end

    end
  end

  describe Patch::DropIndex do
    let(:table_name) { |ex| unique_table_name("posts", ex) }
    let(:table) { Table.new(table_name) }
    let(:column_with_obsolete_idx) {
      DataColumn.new(:author_name, Cequel::Type[:text], :author_name_idx)
    }

    describe "#new" do
      it "returns #{described_class}" do
        expect(
          described_class.new(table, column_with_obsolete_idx)
        ).to be_kind_of described_class
      end
    end

    subject { described_class.new(table, column_with_obsolete_idx) }

    describe "#to_cql" do
      it "drops index" do
        expect( subject.to_cql ).to match /drop +index/i
        expect( subject.to_cql ).to match /"?#{column_with_obsolete_idx.index_name}"?/i
      end
    end

    describe "#index_name" do
      it "returns the index name" do
        expect( subject.index_name ).to eq column_with_obsolete_idx.index_name
      end
    end
  end

  describe Patch::AddIndex do
    let(:table_name) { |ex| unique_table_name("posts", ex) }
    let(:table) { Table.new(table_name) }
    let(:column) {
      DataColumn.new(:author_name, Cequel::Type[:text], :author_name_idx)
    }

    describe "#new" do
      it "returns #{described_class}" do
        expect(
          described_class.new(table, column)
        ).to be_kind_of described_class
      end
    end

    subject { described_class.new(table, column) }

    describe "#to_cql" do
      it "creates the index" do
        expect( subject.to_cql ).to match /create +index/i
        expect( subject.to_cql ).to match /"#{column.index_name}"/i
        expect( subject.to_cql ).to match /( *"#{column.name}" *)/i
      end
    end

    describe "#index_name" do
      it "returns the index name" do
        expect( subject.index_name ).to eq column.index_name
      end
    end

    describe "#column" do
      it "returns the column" do
        expect( subject.column ).to eq column
      end
    end
  end

    describe Patch::AddColumn do
    let(:table_name) { |ex| unique_table_name("posts", ex) }
    let(:table) { Table.new(table_name) }
    let(:column) {
      DataColumn.new(:author_name, Cequel::Type[:text])
    }

    describe "#new" do
      it "returns #{described_class}" do
        expect(
          described_class.new(table, column)
        ).to be_kind_of described_class
      end
    end

    subject { described_class.new(table, column) }

    describe "#to_cql" do
      it "adds the column" do
        expect( subject.to_cql ).to match /alter +table/i
        expect( subject.to_cql ).to match /"#{table_name}"/i
        expect( subject.to_cql ).to match /add +"#{column.name}"/i
      end
    end

    describe "#column" do
      it "returns the column" do
        expect( subject.column ).to eq column
      end
    end
    end
end
