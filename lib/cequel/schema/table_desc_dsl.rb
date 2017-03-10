# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # Implements a DSL used to describe CQL tables.
    #
    # Examples
    #
    #   TableDescDsl.new("posts").eval do
    #     partition_key :blog_subdomain, :text
    #     key :slug, :text
    #     column :body, :text
    #     set :author_names, :text
    #     list :comments, :text
    #     map :something_contrived, :text, :text
    #   end
    #
    class TableDescDsl < BasicObject
      extend ::Cequel::Util::Forwardable

      # Intialize a new instance
      #
      # table_name - The name of the table being described.
      protected def initialize(table_name)
        @table = Table.new(table_name)
      end

      attr_reader :table

      # Returns a Table object built by evaluating the provided block.
      #
      # Yields nothing but block is instance_evaled so it as access to
      #   all the methods of the instance.
      def eval(&desc_block)
        instance_eval(&desc_block)

        @table
      end

      # Describe (one of) the partition key(s) of the table.
      #
      # name - The name of the column.
      # type - The type of the column. Either a `Cequel::Type` or a symbol.
      #   See `Cequel::Type`.
      #
      def partition_key(name, type)
        table.add_column PartitionKey.new(name, type(type))
      end


      # Describe (one of) the key(s) of the table.
      #
      # name - The name of the column
      # type - The type of the column.  Either a `Cequel::Type` or a symbol.
      #   See `Cequel::Type`.
      # clustering_order - `:asc` or `:desc`. Only meaningful for cluster
      #   keys. Leave nil for partition keys.
      #
      def key(name, type, clustering_order = nil)
        (fail ArgumentError, "Can't set clustering order for partition key #{name}") if
          clustering_order && !table.has_partition_key?

        column_desc = if table.has_partition_key?
                        ClusteringColumn.new(name, type(type), clustering_order)
                      else
                        PartitionKey.new(name, type(type))
                      end

        table.add_column column_desc
      end

      # Describe a column of the table
      #
      # name - The name of the column.
      # type - The type of the column. Either a `Cequel::Type` or a symbol.
      #   See `Cequel::Type`.
      # options
      #   :index - name of a secondary index to apply to the column, or
      #     `true` to infer an index name by convention
      #
      def column(name, type, options = {})
        table.add_column DataColumn.new(name, type(type),
                                        figure_index_name(name, options.fetch(:index, nil)))
      end

      # Describe a column of type list.
      #
      # name - The name of the column.
      # type - The type of the elements of this column. Either a
      #   `Cequel::Type` or a symbol. See `Cequel::Type`.
      #
      def list(name, type)
        table.add_column List.new(name, type(type))
      end

      # Describe a column of type set.
      #
      # name - The name of the column.
      # type - The type of the members of this column. Either a
      #  `Cequel::Type` or a symbol. See `Cequel::Type`.
      #
      def set(name, type)
        table.add_column Set.new(name, type(type))
      end

      # Describe a column of type map.
      #
      # name - The name of the column.
      # key_type - The type of the keys of this column. Either a
      #   `Cequel::Type` or a symbol. See `Cequel::Type`.
      # value_type - The type of the values of this column. Either a
      #   `Cequel::Type` or a symbol. See `Cequel::Type`.
      def map(name, key_type, value_type)
        table.add_column Map.new(name, type(key_type), type(value_type))
      end

      # Describe property of the table.
      #
      # name - name of property.
      # value - value of property.
      #
      # See `STORAGE_PROPERTIES` List of storage property names
      # See http://cassandra.apache.org/doc/cql3/CQL.html#createTableOptions
      #   list of CQL3 table storage properties
      #
      def with(name, value)
        table.add_property TableProperty.build(name, value)
      end

      #
      # Direct that this table use "compact storage". This is primarily useful
      # for backwards compatibility with legacy CQL2 table schemas.
      #
      # @return [void]
      #
      def compact_storage
        table.compact_storage = true
      end

      protected

      def type(type)
        type = :int if type == :enum

        ::Cequel::Type[type]
      end

      def figure_index_name(column_name, idx_opt)
        case idx_opt
        when true
          :"#{table.name}_#{column_name}_idx"
        when false, nil
          nil
        else
          idx_opt
        end
      end
    end
  end
end
