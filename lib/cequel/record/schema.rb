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
        self.table_name = name.demodulize.tableize.to_sym unless name.nil? || self.table_name.present?
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
          fail MissingTableNameError unless table_name

          patch =
            begin
              existing_table_descriptor = Cequel::Schema::TableReader.read(connection,
                                                                           table_name)

              return if existing_table_descriptor.materialized_view?

              Cequel::Schema::TableDiffer.new(existing_table_descriptor,
                                              table_schema)
                .call

            rescue NoSuchTableError
              Cequel::Schema::TableWriter.new(table_schema)
            end

          patch.statements.each { |stmt| connection.execute(stmt) }
        end

        #
        # Read the current state of this record's table in Cassandra from the
        # database.
        #
        # @return [Schema::Table] the current schema assigned to this record's
        #   table in the database
        #
        def read_schema
          table_reader.read
        end

        #
        # @return [Schema::Table] the schema as defined by the columns
        #   specified in the class definition
        #
        def table_schema
          dsl.table
        end

        protected

        def dsl
          @dsl ||= Cequel::Schema::TableDescDsl.new(table_name)
        end

        def key(name, type, options = {})
          super
          if options[:partition]
            dsl.partition_key(name, type)
          else
            dsl.key(name, type, options[:order])
          end
        end

        def column(name, type, options = {})
          super
          dsl.column(name, type, options)
        end

        def list(name, type, options = {})
          super
          dsl.list(name, type)
        end

        def set(name, type, options = {})
          super
          dsl.set(name, type)
        end

        def map(name, key_type, value_type, options = {})
          super
          dsl.map(name, key_type, value_type)
        end

        def table_property(name, value)
          dsl.with(name, value)
        end

        def compact_storage
          dsl.compact_storage
        end
      end

      protected

      def_delegator 'self.class', :table_schema
      protected :table_schema
    end
  end
end
