# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # DSL for the construction of a DELETE statement comprising multiple
    # operations (e.g. deleting a column value, deleting an element from a
    # list, etc.)
    #
    #
    # @note This class should not be instantiated directly
    # @see DataSet#delete
    # @see
    #   http://cassandra.apache.org/doc/cql3/CQL.html#deleteStmt
    #   CQL documentation for DELETE
    # @since 1.0.0
    #
    class Deleter < Writer
      private

      def write_to_statement(statement, options)
        statement.append("DELETE FROM #{table_name}")
        statement.append(generate_upsert_options(options))
      end

      def empty?
        false
      end
    end
  end
end
