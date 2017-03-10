# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # Utility object that calculates a `Patch` to transform one `Table`
    # into another.
    #
    # Currently this support adding columns, adding and removing indexes, and
    # setting properties on the table. Any other changes are prohibited due to
    # CQL limitation or data integrity concerns. A `InvalidSchemaMigration` will
    # be raised if unsupported changes are detected.
    #
    class TableDiffer
      protected def initialize(table_a, table_b)
        @table_a = table_a
        @table_b = table_b
      end

      # Returns a Patch that will transform table_a in to table_b.
      #
      def call
        (fail InvalidSchemaMigration, "Table renames are not supported") if
          table_a.name != table_b.name
        (fail InvalidSchemaMigration, "Changes to key structure is not allowed") if
          keys_changed?
        (fail InvalidSchemaMigration, "Type changes are not allowed") if any_types_changed?

        Patch.new(figure_changes)
      end

      protected

      attr_reader :table_a, :table_b

      def figure_changes
        column_changes + property_changes
      end

      def property_changes
        if properties_changed? && table_b.properties.any?
          [Patch::SetTableProperties.new(table_b)]
        else
          []
        end
      end

      def column_changes
        renames
          .map { |old_and_new_c| Patch::RenameColumn.new(table_b, *old_and_new_c) } +
          added_columns
            .map { |new_c| Patch::AddColumn.new(table_b, new_c) } +
          added_indexes
            .map { |col_with_new_idx| Patch::AddIndex.new(table_b, col_with_new_idx) } +
          dropped_indexes
            .map { |col_with_old_idx| Patch::DropIndex.new(table_b, col_with_old_idx) }
      end

      def renames
        table_a.clustering_columns
          .zip(table_b.clustering_columns)
          .select { |ks| ks[0].name != ks[1].name }
      end

      def added_columns
        table_b
          .data_columns
          .reject{|c_a| table_a.data_columns.any?{|c_b| c_b.name == c_a.name } }
      end

      def added_indexes
        table_b.columns
          .select(&:indexed?)
          .reject{|c| table_a.has_column?(c.name) &&
                    table_a.column(c.name).indexed? } # ignore still indexed columns
      end

      def dropped_indexes
        table_a.columns
          .select(&:indexed?)
          .reject{|c| !table_b.has_column?(c.name) } # ignore "dropped" columns
          .reject{|c| table_b.column(c.name).indexed? }
      end

      def any_types_changed?
        table_a.columns
          .select{|c_a| table_b.has_column?(c_a.name) }
          .any?{|c_a| table_b.column(c_a.name).type != c_a.type}
      end

      def keys_changed?
        table_a.partition_key_columns != table_b.partition_key_columns ||
          table_a.clustering_columns.zip(table_b.clustering_columns)
            .any? { |ks| !cluster_keys_compatible?(*ks) }
      end

      def cluster_keys_compatible?(key_a, key_b)
        return false if key_a.blank? or key_b.blank?

        key_a.type == key_b.type &&
          key_a.clustering_order == key_b.clustering_order
      end

      def properties_changed?
        p_a = table_a.properties.values
        p_b = table_b.properties.values

        ((p_a | p_b) - (p_a & p_b)).any?
      end

    end
  end
end
