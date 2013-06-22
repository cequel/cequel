module Cequel

  module Model

    module Dirty

      extend ActiveSupport::Concern

      included do
        include ActiveModel::Dirty
        include ChangedAttributesWithIndifferentAccess
      end

      module ClassMethods

        def column(name, type, options = {})
          define_attribute_method(name)
          super
        end

      end

      def save
        super.tap do
          @previously_changed = changes
          changed_attributes.clear
        end
      end

      def _hydrate(row)
        super.tap { changed_attributes.clear }
      end

      private

      def write_attribute(name, value)
        attribute_will_change!(name) if value != read_attribute(name)
        super
      end

    end

    module ChangedAttributesWithIndifferentAccess

      def changed_attributes
        @changed_attributes ||= HashWithIndifferentAccess.new
      end

    end

  end

end
