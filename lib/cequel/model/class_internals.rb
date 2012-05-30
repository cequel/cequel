module Cequel

  module Model

    #
    # @private
    #
    class ClassInternals

      attr_accessor :key, :current_scope
      attr_reader :columns, :associations, :index_preference

      def initialize(clazz)
        @clazz = clazz
        @columns, @associations = {}, {}
        @index_preference = []
        @lock = Monitor.new
      end

      def add_column(name, type, options = {})
        @columns[name] = Column.new(name, type, options)
      end

      def type_column
        @columns[:class_name]
      end

      def column_family_name
        @column_family_name ||= @clazz.name.tableize
      end

      def base_class
        @clazz
      end

      def association(name)
        associations[name]
      end

      def synchronize(&block)
        @lock.synchronize(&block)
      end

    end

  end

end
