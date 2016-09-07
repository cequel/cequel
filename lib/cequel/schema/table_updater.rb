# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # Encapsulates a series of schema modification statements that can be
    # applied to an existing table
    #
    class TableUpdater
      #
      # Construct a table updater and apply the schema modifications to the
      # given table.
      #
      # @param (see #initialize)
      # @yieldparam updater [TableUpdater] instance of updater whose
      #   modifications will be applied to the named table
      # @return [void]
      #
      def self.apply(keyspace, table_name, &block)
        new(keyspace, table_name).tap(&block).apply
      end

      #
      # @param keyspace [Metal::Keyspace] keyspace containing the table
      # @param table_name [Symbol] name of the table to modify
      # @private
      #
      def initialize(keyspace, table_name)
        @keyspace, @table_name = keyspace, table_name
        @statements = []
      end
      private_class_method :new

      #
      # Apply the schema modifications to the table schema in the database
      #
      # @return [void]
      #
      # @api private
      #
      def apply
        statements.each { |statement| keyspace.execute(statement) }
      end

      #
      # Add a column to the table
      #
      # @param name [Symbol] the name of the column
      # @param type [Symbol,Type] the type of the column
      # @return [void]
      #
      def add_column(name, type)
        add_data_column(Column.new(name, type(type)))
      end

      #
      # Add a list to the table
      #
      # @param name [Symbol] the name of the list
      # @param type [Symbol,Type] the type of the list elements
      # @return [void]
      #
      def add_list(name, type)
        add_data_column(List.new(name, type(type)))
      end

      #
      # Add a set to the table
      #
      # @param name [Symbol] the name of the set
      # @param type [Symbol,Type] the type of the set elements
      # @return [void]
      #
      def add_set(name, type)
        add_data_column(Set.new(name, type(type)))
      end

      #
      # Add a map to the table
      #
      # @param name [Symbol] the name of the map
      # @param key_type [Symbol,Type] the type of the map's keys
      # @param value_type [Symbol,Type] the type of the map's values
      # @return [void]
      #
      def add_map(name, key_type, value_type)
        add_data_column(Map.new(name, type(key_type), type(value_type)))
      end

      #
      # Change an existing column's type
      #
      # @param name [Symbol] the name of the column
      # @param type [Symbol,Type] the new type of the column
      # @return [void]
      #
      # @note Changing the type of a CQL column does not modify the data
      #   currently stored in the column. Thus, client-side handling is needed
      #   to convert old values to the new type at read time. Cequel does not
      #   currently support this functionality, although it may in the future.
      #   Altering column types is not recommended.
      #
      def change_column(name, type)
        alter_table("ALTER #{name} TYPE #{type(type).cql_name}")
      end

      #
      # Rename a column
      #
      # @param old_name [Symbol] the current name of the column
      # @param new_name [Symbol] the new name of the column
      # @return [void]
      #
      def rename_column(old_name, new_name)
        alter_table(%(RENAME "#{old_name}" TO "#{new_name}"))
      end

      #
      # Change one or more table storage properties
      #
      # @param options [Hash] map of property names to new values
      # @return [void]
      #
      # @see Table#add_property
      #
      def change_properties(options)
        properties = options
          .map { |name, value| TableProperty.build(name, value).to_cql }
        alter_table("WITH #{properties.join(' AND ')}")
      end

      #
      # Create a secondary index
      #
      # @param column_name [Symbol] name of the column to add an index on
      # @param index_name [Symbol] name of the index; will be inferred from
      #   convention if nil
      # @return [void]
      #
      def create_index(column_name, index_name = nil)
        index_name ||= "#{table_name}_#{column_name}_idx"
        statements <<
          "CREATE INDEX #{index_name} ON #{table_name} (#{column_name})"
      end

      #
      # Remove a secondary index
      #
      # @param index_name [Symbol] the name of the index to remove
      # @return [void]
      #
      def drop_index(index_name)
        statements << "DROP INDEX IF EXISTS #{index_name}"
      end

      # @!visibility protected
      def add_data_column(column)
        add_column_statement(column)
      end

      protected

      attr_reader :keyspace, :table_name, :statements

      private

      def alter_table(statement)
        statements << "ALTER TABLE #{table_name} #{statement}"
      end

      def add_column_statement(column)
        alter_table("ADD #{column.to_cql}")
      end

      def type(type)
        ::Cequel::Type[type]
      end
    end
  end
end
