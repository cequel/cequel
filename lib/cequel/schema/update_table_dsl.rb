module Cequel

  module Schema

    class UpdateTableDSL < BasicObject

      def self.apply(updater, &block)
        dsl = new(updater)
        dsl.instance_eval(&block)
      end

      def initialize(updater)
        @updater = updater
      end

      def add_column(name, type)
        @updater.add_column(name, ::Cequel::Type[type])
      end

      def add_list(name, type)
        @updater.add_list(name, ::Cequel::Type[type])
      end

      def add_set(name, type)
        @updater.add_set(name, ::Cequel::Type[type])
      end

      def add_map(name, key_type, value_type)
        @updater.add_map(name, ::Cequel::Type[key_type],
                         ::Cequel::Type[value_type])
      end

      def change_column(name, type)
        @updater.change_column(name, ::Cequel::Type[type])
      end

      def rename_column(old_name, new_name)
        @updater.rename_column(old_name, new_name)
      end

      def change_properties(options)
        @updater.change_properties(options)
      end
      alias_method :change_options, :change_properties

      def create_index(column_name, index_name = nil)
        @updater.create_index(column_name, index_name)
      end
      alias_method :add_index, :create_index

      def drop_index(index_name)
        @updater.drop_index(index_name)
      end
      alias_method :remove_index, :drop_index

    end

  end

end
