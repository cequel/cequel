module Cequel

  module Serialization

    class Base

      attr_reader :obj

      def initialize(object)
        @obj = object
      end

    end

  end

end
