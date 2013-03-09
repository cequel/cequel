module Cequel

  module Schema

    class TableSynchronizer

      def initialize(existing, updated)
        @existing, @updated = existing, updated
      end

      def updater
        return @updater if @updater
        @updater = TableUpdater.new(@existing.name)
        update_keys
        update_columns
        update_properties
        @updater
      end

      private

      def update_keys
        each_key_pair do |old_key, new_key|
          if old_key.type != new_key.type
            raise InvalidSchemaMigration,
              "Can't change type of key column #{old_key.name} from #{old_key.type} to #{new_key.type}"
          end
          if old_key.name != new_key.name
            @updater.rename_column(old_key.name, new_key.name)
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
        @updater.add_data_column(column)
        if column.indexed?
          @updater.create_index(column.name, column.index_name)
        end
      end

      def update_column(old_column, new_column)
        if old_column.type != new_column.type
          @updater.change_column(new_column.name, new_column.type)
        end
        if !old_column.indexed? && new_column.indexed?
          @updater.create_index(new_column.name, new_column.index_name)
        elsif old_column.indexed? && !new_column.indexed?
          @updater.drop_index(old_column.index_name)
        end
      end

      def update_properties
        changes = {}
        @updated.properties.each_pair do |name, new_property|
          old_property = @existing.property(name)
          if old_property != new_property.value
            changes[name] = new_property.value
          end
        end
        @updater.change_properties(changes) if changes.any?
      end

      def each_key_pair(&block)
        if @existing.partition_keys.length != @updated.partition_keys.length
          raise InvalidSchemaMigration,
            "Existing partition keys #{@existing.partition_keys.map { |key| key.name }.join(',')} differ from specified partition keys #{@updated.partition_keys.map { |key| key.name }.join(',')}"
        end
        if @existing.nonpartition_keys.length != @updated.nonpartition_keys.length
          raise InvalidSchemaMigration,
            "Existing clustering keys #{@existing.nonpartition_keys.map { |key| key.name }.join(',')} differ from specified clustering keys #{@updated.nonpartition_keys.map { |key| key.name }.join(',')}"
        end
        @existing.partition_keys.zip(@updated.partition_keys, &block)
        @existing.nonpartition_keys.zip(@updated.nonpartition_keys, &block)
      end

      def each_column_pair(&block)
        old_columns = @existing.data_columns.index_by { |col| col.name }
        new_columns = @updated.data_columns.index_by { |col| col.name }
        all_column_names = (old_columns.keys + new_columns.keys).uniq!
        all_column_names.each do |name|
          yield old_columns[name], new_columns[name]
        end
      end

    end

  end

end
