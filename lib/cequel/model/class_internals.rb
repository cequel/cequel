module Cequel

  module Model

    #
    # @private
    #
    class ClassInternals

      attr_accessor :key, :current_scope
      attr_reader :columns

      def initialize(clazz)
        @clazz = clazz
        @columns = {}
      end

      def add_column(name, type)
        @columns[name] = Column.new(name, type)
      end

      def type_column
        @columns[:type]
      end

      def column_family_name
        @clazz.name.tableize
      end

      def base_class
        @clazz
      end

    end

  end

end
