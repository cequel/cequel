# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # Builder for `UPDATE` statement containing heterogeneous operations (set
    # columns, atomically mutate collections)
    #
    # @see DataSet#update
    # @see Deleter
    # @see
    #   http://cassandra.apache.org/doc/cql3/CQL.html#updateStmt
    #   CQL UPDATE documentation
    # @since 1.0.0
    #
    class Updater < Writer
      #
      # @see Writer#initialize
      #
      def initialize(*)
        @column_updates = {}
        super
      end

      #
      # Directly set column values
      #
      # @param data [Hash] map of column names to values
      # @return [void]
      #
      # @see DataSet#update
      #
      def set(data)
        data.each_pair do |column, value|
          column_updates[column.to_sym] = value
        end
      end

      #
      # Prepend elements to a list column
      #
      # @param column [Symbol] column name
      # @param elements [Array<Object>] elements to prepend
      # @return [void]
      #
      # @see DataSet#list_prepend
      #
      def list_prepend(column, elements)
        statements << "#{column} = [?] + #{column}"
        bind_vars << elements
      end

      #
      # Append elements to a list column
      #
      # @param column [Symbol] column name
      # @param elements [Array] elements to append
      # @return [void]
      #
      # @see DataSet#list_append
      #
      def list_append(column, elements)
        statements << "#{column} = #{column} + [?]"
        bind_vars << elements
      end

      #
      # Remove all occurrences of an element from a list
      #
      # @param column [Symbol] column name
      # @param value value to remove
      # @return [void]
      #
      # @see DataSet#list_remove
      #
      def list_remove(column, value)
        statements << "#{column} = #{column} - [?]"
        bind_vars << value
      end

      #
      # Replace a list item at a given position
      #
      # @param column [Symbol] column name
      # @param index [Integer] index at which to replace value
      # @param value new value for position
      # @return [void]
      #
      # @see DataSet#list_replace
      #
      def list_replace(column, index, value)
        statements << "#{column}[#{index}] = ?"
        bind_vars << value
      end

      #
      # Add elements to a set
      #
      # @param column [Symbol] column name
      # @param values [Set] elements to add to set
      # @return [void]
      #
      # @see DataSet#set_add
      #
      def set_add(column, values)
        statements << "#{column} = #{column} + {?}"
        bind_vars << values
      end

      #
      # Remove elements from a set
      #
      # @param column [Symbol] column name
      # @param values [Set] elements to remove from set
      # @return [void]
      #
      # @see DataSet#set_remove
      #
      def set_remove(column, values)
        statements << "#{column} = #{column} - {?}"
        bind_vars << ::Kernel.Array(values)
      end

      #
      # Add or update elements in a map
      #
      # @param column [Symbol] column name
      # @param updates [Hash] map of keys to values to update in map
      # @return [void]
      #
      # @see DataSet#map_update
      #
      def map_update(column, updates)
        binding_pairs = ::Array.new(updates.length) { '?:?' }.join(',')
        statements << "#{column} = #{column} + {#{binding_pairs}}"
        bind_vars.concat(updates.flatten)
      end

      private

      attr_reader :column_updates

      def empty?
        super && column_updates.empty?
      end

      def write_to_statement(statement, options)
        all_statements, all_bind_vars = statements_with_column_updates
        statement.append("UPDATE #{table_name}")
          .append(generate_upsert_options(options))
          .append(" SET ")
          .append(all_statements.join(', '), *all_bind_vars)
      end

      def statements_with_column_updates
        all_statements, all_bind_vars = statements.dup, bind_vars.dup
        column_updates.each_pair do |column, value|
          prepare_upsert_value(value) do |binding, *values|
            all_statements << "#{column} = #{binding}"
            all_bind_vars.concat(values)
          end
        end
        [all_statements, all_bind_vars]
      end
    end
  end
end
