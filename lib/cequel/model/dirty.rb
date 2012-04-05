require 'active_model/dirty'

module Cequel

  module Model

    module Dirty

      extend ActiveSupport::Concern

      included do
        include ActiveModel::Dirty
      end

      module ClassMethods

        def column(name, type)
          define_attribute_method(name)
          super
        end

      end

      def save
        super
        @previously_changed = changes
        changed_attributes.clear
      end

      private

      def write_attribute(name, value)
        attribute_will_change!(name) if value != read_attribute(name)
        super
      end

    end

  end

end
