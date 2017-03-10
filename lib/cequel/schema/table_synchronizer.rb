# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # Synchronize a table schema in the database with a desired table schema
    #
    # @see .apply
    # @see Keyspace#synchronize_table
    #
    class TableSynchronizer
      # @return [Table] table as it is currently defined
      # @api private
      attr_reader :existing
      # @return [Table] table schema as it is desired
      # @api private
      attr_reader :updated
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
        patch = if existing
                  patch = TableDiffer.new(existing, updated).call
                else
                  TableWriter.new(updated)
                end
 
        patch.statements.each { |stmt| keyspace.execute(stmt) }
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
      # @raise (see MigrationValidator#validate!)
      #
      # @api private
      #
      def apply
        validate!
        update_keys
        update_columns
        update_properties
      end

      #
      # Iterate over pairs of (old_key, new_key)
      #
      # @yieldparam old_key [Column] key in existing schema
      # @yieldparam new_key [Column] corresponding key in updated schema
      # @return [void]
      #
      # @api private
      #
      def each_key_pair(&block)
        existing.key_columns.zip(updated.key_columns, &block)
      end

      #
      # Iterate over pairs of (old_column, new_column)
      #
      # @yieldparam old_column [Column] column in existing schema
      # @yieldparam new_column [Column] corresponding column in updated schema
      # @return [void]
      #
      # @api private
      #
      def each_data_column_pair(&block)
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

      #
      # Iterate over pairs of (old_clustering_column, new_clustering_column)
      #
      # @yieldparam old_clustering_column [Column] key in existing schema
      # @yieldparam new_clustering_column [Column] corresponding key in updated
      #   schema
      # @return [void]
      #
      # @api private
      #
      def each_clustering_column_pair(&block)
        existing.clustering_columns.zip(updated.clustering_columns, &block)
      end

      protected

      attr_reader :updater

      private

      def update_keys
        each_key_pair do |old_key, new_key|
          if old_key.name != new_key.name
            updater.rename_column(old_key.name || :column1, new_key.name)
          end
        end
      end

      def update_columns
        each_data_column_pair do |old_column, new_column|
          if old_column.nil?
            add_column(new_column)
          elsif new_column
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

      def validate!
        MigrationValidator.validate!(self)
      end
    end
  end
end
