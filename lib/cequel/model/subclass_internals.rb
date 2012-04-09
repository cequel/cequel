module Cequel

  module Model

    class SubclassInternals < ClassInternals

      def initialize(clazz, super_internals)
        @clazz, @super = clazz, super_internals
        @columns = {}
      end

      def key
        @super.key
      end

      def columns
        @super.columns.merge(@columns)
      end

      def type_column
        @super.type_column
      end

      def column_family_name
        @super.column_family_name
      end

      def base_class
        @super.base_class
      end

    end
    
  end

end
