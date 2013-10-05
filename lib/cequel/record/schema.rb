module Cequel

  module Record

    module Schema

      extend ActiveSupport::Concern
      extend Forwardable

      included do
        class_attribute :table_name, :instance_writer => false
        self.table_name = name.tableize.to_sym unless name.nil?
      end

      module ClassMethods
        extend Forwardable

        def_delegators :table_schema, :columns, :key_columns, :key_column_names,
          :partition_key_columns, :clustering_columns, :compact_storage?
        def_delegator :table_schema, :column, :reflect_on_column

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

        protected

        def key(name, type, options = {})
          super
          table_schema.add_key(name, type)
        end

        def column(name, type, options = {})
          super
          table_schema.add_data_column(name, type, options[:index])
        end

        def list(name, type, options = {})
          super
          table_schema.add_list(name, type)
        end

        def set(name, type, options = {})
          super
          table_schema.add_set(name, type)
        end

        def map(name, key_type, value_type, options = {})
          super
          table_schema.add_map(name, key_type, value_type)
        end

        def table_property(name, value)
          table_schema.add_property(name, value)
        end

        def compact_storage
          table_schema.compact_storage = true
        end

      end

      protected
      def_delegator 'self.class', :table_schema
      protected :table_schema

    end

  end

end
