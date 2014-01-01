module Cequel
  module Schema
    #
    # Synchronize a table schema in the database with a desired table schema
    #
    # @see .apply
    # @see Keyspace#synchronize_table
    #
    class TableSynchronizer
      #
      # Takes an existing table schema read from the database, and a desired
      # schema for that table. Modifies the table schema in the database to
      # match the desired schema, or creates the table as specified if it does
      # not yet exist
      #
      # @param keyspace [Metal::Keyspace] keyspace that contains table
      # @param existing [Table] table schema as it is currently defined
      # @param updated [Table] table schema as it is desired
      # @return [void]
      # @raise (see #apply)
      #
      def self.apply(keyspace, existing, updated)
        if existing
          TableUpdater.apply(keyspace, existing.name) do |updater|
            new(updater, existing, updated).apply
          end
        else
          TableWriter.apply(keyspace, updated)
        end
      end

      #
      # @param updater [TableUpdater] table updater to hold schema
      #   modifications
      # @param existing [Table] table schema as it is currently defined
      # @param updated [Table] table schema as it is desired
      # @return [void]
      # @private
      #
      def initialize(updater, existing, updated)
        @updater, @existing, @updated = updater, existing, updated
      end
      private_class_method :new

      #
      # Apply the changes needed to synchronize the schema in the database with
      # the desired schema
      #
      # @return [void]
      # @raise [InvalidSchemaMigration] if it is impossible to modify existing
      #   table to match desired schema
      #
      # @api private
      #
      def apply
        update_keys
        update_columns
        update_properties
      end

      protected

      attr_reader :updater, :existing, :updated

      private

      def update_keys
        each_key_pair do |old_key, new_key|
          if old_key.type != new_key.type
            fail InvalidSchemaMigration,
                 "Can't change type of key column #{old_key.name} from " \
                 "#{old_key.type} to #{new_key.type}"
          end
          if old_key.name != new_key.name
            updater.rename_column(old_key.name || :column1, new_key.name)
          end
        end
      end

      def update_columns
        each_column_pair do |old_column, new_column|
          if old_column.nil?
            add_column(new_column)
          elsif new_column
            assert_same_column_type!(old_column, new_column)
            update_column(old_column, new_column)
            update_index(old_column, new_column)
          end
        end
      end

      def add_column(column)
        updater.add_data_column(column)
        updater.create_index(column.name, column.index_name) if column.indexed?
      end

      def update_column(old_column, new_column)
        if old_column.name != new_column.name
          updater.rename_column(old_column.name || :value, new_column.name)
        end
        if old_column.type != new_column.type
          updater.change_column(new_column.name, new_column.type)
        end
      end

      def update_index(old_column, new_column)
        if !old_column.indexed? && new_column.indexed?
          updater.create_index(new_column.name, new_column.index_name)
        elsif old_column.indexed? && !new_column.indexed?
          updater.drop_index(old_column.index_name)
        end
      end

      def update_properties
        changes = {}
        updated.properties.each_pair do |name, new_property|
          old_property = existing.property(name)
          if old_property != new_property.value
            changes[name] = new_property.value
          end
        end
        updater.change_properties(changes) if changes.any?
      end

      def each_key_pair(&block)
        assert_keys_match!
        existing.key_columns.zip(updated.key_columns, &block)
      end

      def each_column_pair(&block)
        if existing.compact_storage? && existing.clustering_columns.any?
          yield existing.data_columns.first, updated.data_columns.first
        else
          old_columns = existing.data_columns.index_by { |col| col.name }
          new_columns = updated.data_columns.index_by { |col| col.name }
          all_column_names = (old_columns.keys + new_columns.keys).tap(&:uniq!)
          all_column_names.each do |name|
            yield old_columns[name], new_columns[name]
          end
        end
      end

      private

      def assert_keys_match!
        assert_partition_keys_match!
        assert_clustering_columns_match!
      end

      def assert_partition_keys_match!
        if existing.partition_key_column_count !=
            updated.partition_key_column_count

          fail InvalidSchemaMigration,
               "Existing partition keys " \
               "#{existing.partition_key_column_names.join(',')} " \
               "differ from specified partition keys " \
               "#{updated.partition_key_column_names.join(',')}"
        end
      end

      def assert_clustering_columns_match!
        if existing.clustering_column_count != updated.clustering_column_count
          fail InvalidSchemaMigration,
               "Existing clustering columns " \
               "#{existing.clustering_column_names.join(',')} " \
               "differ from specified clustering keys " \
               "#{updated.clustering_column_names.join(',')}"
        end
      end

      def assert_same_column_type!(old_column, new_column)
        if old_column.class != new_column.class
          fail InvalidSchemaMigration,
               "Can't change #{old_column.name} from " \
               "#{old_column.class.name.demodulize} to " \
               "#{new_column.class.name.demodulize}"
        end
      end
    end
  end
end
