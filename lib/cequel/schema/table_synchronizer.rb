module Cequel

  module Schema

    class TableSynchronizer

      def self.apply(keyspace, existing, updated)
        if existing
          TableUpdater.apply(keyspace, existing.name) do |updater|
            new(updater, existing, updated).apply
          end
        else
          TableWriter.apply(keyspace, updated)
        end
      end

      def initialize(updater, existing, updated)
        @updater, @existing, @updated = updater, existing, updated
      end
      private_class_method :new

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
            raise InvalidSchemaMigration,
              "Can't change type of key column #{old_key.name} from #{old_key.type} to #{new_key.type}"
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
            if old_column.class != new_column.class
              raise InvalidSchemaMigration,
                "Can't change #{old_column.name} from #{old_column.class.name.demodulize} to #{new_column.class.name.demodulize}"
            end
            update_column(old_column, new_column)
          end
        end
      end

      def add_column(column)
        updater.add_data_column(column)
        if column.indexed?
          updater.create_index(column.name, column.index_name)
        end
      end

      def update_column(old_column, new_column)
        if old_column.name != new_column.name
          updater.rename_column(old_column.name || :value, new_column.name)
        end
        if old_column.type != new_column.type
          updater.change_column(new_column.name, new_column.type)
        end
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
        if existing.partition_key_columns.length != updated.partition_key_columns.length
          raise InvalidSchemaMigration,
            "Existing partition keys #{existing.partition_key_columns.map { |key| key.name }.join(',')} differ from specified partition keys #{updated.partition_key_columns.map { |key| key.name }.join(',')}"
        end
        if existing.clustering_columns.length != updated.clustering_columns.length
          raise InvalidSchemaMigration,
            "Existing clustering keys #{existing.clustering_columns.map { |key| key.name }.join(',')} differ from specified clustering keys #{updated.clustering_columns.map { |key| key.name }.join(',')}"
        end
        existing.partition_key_columns.zip(updated.partition_key_columns, &block)
        existing.clustering_columns.zip(updated.clustering_columns, &block)
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

    end

  end

end
