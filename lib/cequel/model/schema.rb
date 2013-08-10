module Cequel

  module Model

    module Schema

      extend ActiveSupport::Concern

      module ClassMethods

        def synchronize_schema
          Cequel::Schema::TableSynchronizer.apply(connection, read_schema, schema)
        end

        def read_schema
          connection.schema.read_table(table_name)
        end

        def schema
          @schema ||= Cequel::Schema::Table.new(table_name)
        end

      end

    end

  end

end
