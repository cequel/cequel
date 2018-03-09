# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # Cequel provides support for dirty attribute tracking via ActiveModel.
    # Modifications to collection columns are registered by this mechanism.
    #
    # @see http://api.rubyonrails.org/classes/ActiveModel/Dirty.html Rails
    #   documentation for ActiveModel::Dirty
    #
    # @since 0.1.0
    #
    module Dirty
      extend ActiveSupport::Concern

      included { include ActiveModel::Dirty }

      # @private
      module ClassMethods
        def key(name, *)
          define_attribute_method(name)
          super
        end

        def column(name, *)
          define_attribute_method(name)
          super
        end

        def set(name, *)
          define_attribute_method(name)
          super
        end

        def list(name, *)
          define_attribute_method(name)
          super
        end

        def map(name, *)
          define_attribute_method(name)
          super
        end
      end

      # @private
      def save(options = {})
        super.tap do |success|
          if success
            @previously_changed = changes
            @changed_attributes.clear
          end
        end
      end

      private

      def write_attribute(name, value)
        column = self.class.reflect_on_column(name)
        fail UnknownAttributeError, "unknown attribute: #{name}" unless column
        value = column.cast(value) unless value.nil?

        if loaded? && value != read_attribute(name)
          __send__("#{name}_will_change!")
        end
        super
      end
    end
  end
end
