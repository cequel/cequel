module Cequel

  module Record

    module Schema

      extend ActiveSupport::Concern

      included do
        class_attribute :table_name, :instance_writer => false
        self.table_name = name.tableize.to_sym unless name.nil?
      end

      module ClassMethods
        extend Forwardable

        def_delegators :table_schema, :key_columns, :key_column_names,
          :partition_key_columns, :clustering_columns

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

      end

    end

  end

end
