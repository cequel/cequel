module Cequel

  module Model

    module Dynamic

      def [](column)
        read_attribute(column)
      end

      def []=(column, value)
        write_attribute(column, value)
      end

      private

      def attribute_change(attr)
        if attribute_changed?(attr)
          if respond_to_without_attributes?(attr)
            super
          else
            [changed_attributes[attr], self[attr]]
          end
        end
      end

    end

  end

end
