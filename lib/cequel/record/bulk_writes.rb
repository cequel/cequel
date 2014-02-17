# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # This module implements bulk update and delete functionality for classes
    # that expose a collection of result rows.
    #
    # @abstract Including modules must implement `key_attributes_for_each_row`,
    #   which should yield successive fully-specified key attributes for each
    #   result row.
    #
    # @since 1.0.0
    #
    module BulkWrites
      #
      # Update all matched records with the given column values, without
      # executing callbacks.
      #
      # @param attributes [Hash] map of column names to values
      # @return [void]
      #
      def update_all(attributes)
        each_data_set { |data_set| data_set.update(attributes) }
      end

      #
      # Delete all matched records without executing callbacks
      #
      # @return [void]
      #
      def delete_all
        each_data_set { |data_set| data_set.delete }
      end

      #
      # Destroy all matched records, executing destroy callbacks for each
      # record.
      #
      # @return [void]
      #
      def destroy_all
        each { |record| record.destroy }
      end

      private

      def each_data_set
        key_attributes_for_each_row.each_slice(100) do |batch|
          connection.batch(unlogged: true) do
            batch.each { |key_attributes| yield table.where(key_attributes) }
          end
        end
      end
    end
  end
end
