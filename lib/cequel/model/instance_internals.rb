module Cequel

  module Model

    #
    # @private
    #
    class InstanceInternals

      attr_accessor :key, :attributes, :persisted

      def initialize(instance)
        @instance = instance
        @attributes = ActiveSupport::HashWithIndifferentAccess.new
      end

    end
    
  end

end
