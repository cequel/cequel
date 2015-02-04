# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # `Cequel::Record` implementations define their own schema in their class
    # definitions. As well as defining attributes on record instances, the
    # column definitions in {Properties} allow a `Cequel::Record` to have a
    # precise internal represntation of its representation as a CQL3 table
    # schema. Further, it is able to check this representation against the
    # actual table defined in Cassandra (if any), and create or modify the
    # schema in Cassandra to match what's defined in code.
    #
    # All the interesting stuff is in the {ClassMethods}.
    #
    # @since 1.0.0
    #
    module Schema
      extend ActiveSupport::Concern
      extend Util::Forwardable

      included do
        class_attribute :table_name, instance_writer: false
        self.table_name = name.demodulize.tableize.to_sym unless name.nil?
      end

      #
      # Methods available on {Record} class singletons to introspect and modify
      # the schema defined in the database
      #
      module ClassMethods
        #
        # @!attr table_name
        #   @return [Symbol] name of the CQL table that backs this record class
        #

        extend Util::Forwardable

        #
        # @!attribute [r] columns
        #   (see Cequel::Schema::Table#columns)
        #
        # @!attribute [r] column_names
        #   (see Cequel::Schema::Table#column_names)
        #
        # @!attribute [r] key_columns
        #   (see Cequel::Schema::Table#key_columns)
        #
        # @!attribute [r] key_column_names
        #   (see Cequel::Schema::Table#key_column_names)
        #
        # @!attribute [r] partition_key_columns
        #   (see Cequel::Schema::Table#partition_key_columns)
        #
        # @!attribute [r] partition_key_column_names
        #   (see Cequel::Schema::Table#partition_key_column_names)
        #
        # @!attribute [r] clustering_columns
        #   (see Cequel::Schema::Table#clustering_columns)
        #
        # @!method compact_storage?
        #   (see Cequel::Schema::Table#compact_storage?)
        #
        def_delegators :table_schema, :columns, :column_names, :key_columns,
                       :key_column_names, :partition_key_columns,
                       :partition_key_column_names, :clustering_columns,
                       :compact_storage?
        #
        # @!method reflect_on_column(name)
        #   (see Cequel::Schema::Table#column)
        #
        def_delegator :table_schema, :column, :reflect_on_column

        #
        # Read the current schema assigned to this record's table from
        # Cassandra, and make any necessary modifications (including creating
        # the table for the first time) so that it matches the schema defined
        # in the record definition
        #
        # @raise (see Schema::TableSynchronizer.apply)
        # @return [void]
        #
        def synchronize_schema
          Cequel::Schema::TableSynchronizer
            .apply(connection, read_schema, table_schema)
        end

        #
        # Read the current state of this record's table in Cassandra from the
        # database.
        #
        # @return [Schema::Table] the current schema assigned to this record's
        #   table in the database
        #
        def read_schema
          fail MissingTableNameError unless table_name

          connection.schema.read_table(table_name)
        end

        #
        # @return [Schema::Table] the schema as defined by the columns
        #   specified in the class definition
        #
        def table_schema
          @table_schema ||= Cequel::Schema::Table.new(table_name)
        end

        protected

        def key(name, type, options = {})
          super
          if options[:partition]
            table_schema.add_partition_key(name, type)
          else
            table_schema.add_key(name, type, options[:order])
          end
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
