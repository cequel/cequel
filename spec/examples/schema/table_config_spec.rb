# -*- encoding : utf-8 -*-
require File.expand_path('../../spec_helper', __FILE__)

module Cequel::Schema
  describe 'Table Configuration' do
    let(:table_name) { |ex| unique_table_name("posts", ex) }

    it 'should error on duplicate tables' do
      expect do
        Table.new(table_name).tap do |t|
          t.add_column PartitionKey.new(:blog_subdomain, Cequel::Type[:text])
          t.add_column ClusteringColumn.new(:slug, Cequel::Type[:text])
          t.add_column DataColumn.new(:body, Cequel::Type[:text])
          t.add_column DataColumn.new(:body, Cequel::Type[:text])
        end
      end.to raise_error ArgumentError
    end
  end
end
