module Cequel

  module Model

    #
    # @private
    #
    class ClassInternals

      attr_accessor :key

      def initialize(clazz)
        @clazz = clazz
      end

    end

  end

end
