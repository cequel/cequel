module Cequel
  module Schema
    #
    # This is a utility class to test that it is possible to perform a given
    # table schema migration
    #
    # @api private
    #
    class MigrationValidator
      extend Forwardable
      #
      # Check for various impossible schema changes and raise if any are found
      #
      # @param (see #initialize)
      # @return [void]
      # @raise (see #validate)
      #
      def self.validate!(synchronizer)
        new(synchronizer).validate!
      end

      #
      # @param synchronizer [TableSynchronizer] the synchronizer to validate
      #
      def initialize(synchronizer)
        @synchronizer = synchronizer
      end

      #
      # Check for various impossible schema changes and raise if any are found
      #
      # @raise [InvalidSchemaMigration] if it is impossible to modify existing
      #   table to match desired schema
      #
      def validate!
        assert_keys_match!
        assert_data_columns_match!
      end

      private

      attr_reader :synchronizer
      def_delegators :synchronizer, :each_key_pair, :each_data_column_pair,
                     :existing, :updated

      def assert_keys_match!
        assert_partition_keys_match!
        assert_clustering_columns_match!
        assert_same_key_types!
      end

      def assert_same_key_types!
        each_key_pair do |old_key, new_key|
          if old_key.type != new_key.type
            fail InvalidSchemaMigration,
              "Can't change type of key column #{old_key.name} from " \
              "#{old_key.type} to #{new_key.type}"
          end
        end
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

      def assert_data_columns_match!
        each_data_column_pair do |old_column, new_column|
          if old_column && new_column
            assert_same_column_type!(old_column, new_column)
          end
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
