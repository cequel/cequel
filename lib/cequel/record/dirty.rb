module Cequel

  module Record

    module Dirty

      extend ActiveSupport::Concern

      included { include ActiveModel::Dirty }

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
        if loaded? && value != read_attribute(name)
          __send__("#{name}_will_change!")
        end
        super
      end

    end

  end

end
