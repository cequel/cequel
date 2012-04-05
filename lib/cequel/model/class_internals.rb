module Cequel

  module Model

    #
    # @private
    #
    class ClassInternals

      attr_accessor :key
      attr_reader :columns

      def initialize(clazz)
        @clazz = clazz
        @columns = {}
      end

      def add_column(name, type)
        @columns[name] = Column.new(name, type)
      end

    end

  end

end
