# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # This is a utility class to test that it is possible to perform a given
    # table schema migration
    #
    # @api private
    #
    class MigrationValidator
      extend Util::Forwardable
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
      def_delegators :synchronizer, :each_key_pair,
                     :each_clustering_column_pair, :each_data_column_pair,
                     :existing, :updated

      def assert_keys_match!
        assert_partition_keys_match!
        assert_clustering_columns_match!
        assert_same_key_types!
        assert_same_clustering_order!
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

      def assert_same_clustering_order!
        each_clustering_column_pair do |old_key, new_key|
          if old_key.clustering_order != new_key.clustering_order
            fail InvalidSchemaMigration,
                 "Can't change the clustering order of #{old_key.name} from " \
                 "#{old_key.clustering_order} to #{new_key.clustering_order}"
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
               "#{updated.clustering_column_names.join(',')} " \
               "for #{existing.name}"
        end
      end

      def assert_data_columns_match!
        each_data_column_pair do |old_column, new_column|
          if old_column && new_column
            assert_valid_type_transition!(old_column, new_column)
            assert_same_column_structure!(old_column, new_column)
          end
        end
      end

      def assert_valid_type_transition!(old_column, new_column)
        if old_column.type != new_column.type
          valid_new_types = old_column.type.compatible_types
          unless valid_new_types.include?(new_column.type)
            fail InvalidSchemaMigration,
                 "Can't change #{old_column.name} from " \
                 "#{old_column.type} to #{new_column.type}. " \
                 "#{old_column.type} columns may only be altered to " \
                 "#{valid_new_types.to_sentence}."
          end
        end
      end

      def assert_same_column_structure!(old_column, new_column)
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
