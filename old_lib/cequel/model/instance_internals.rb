module Cequel

  module Model

    #
    # @private
    #
    class InstanceInternals

      attr_accessor :key, :attributes, :persisted
      attr_reader :associations

      def initialize(instance)
        @instance = instance
        @attributes = ActiveSupport::HashWithIndifferentAccess.new
        @associations = {}
      end

    end
    
  end

end
