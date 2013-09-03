module Cequel

  module Model

    module Schema

      extend ActiveSupport::Concern

      module ClassMethods

        def synchronize_schema
          Cequel::Schema::TableSynchronizer.
            apply(connection, read_schema, table_schema)
        end

        def read_schema
          connection.schema.read_table(table_name)
        end

        def table_schema
          @table_schema ||= Cequel::Schema::Table.new(table_name)
        end

        def local_key_column
          @local_key_column ||= table_schema.key_columns.last
        end

        def primary_keys
          @primary_keys ||= table_schema.partition_keys + table_schema.clustering_columns
        end

      end

    end

  end

end
