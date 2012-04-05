module Cequel

  module Model

    #
    # @private
    #
    class InstanceInternals

      attr_accessor :key

      def initialize(instance)
        @instance = instance
      end

    end
    
  end

end
