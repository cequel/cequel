module Cequel

  module Model

    module Associations

      extend ActiveSupport::Concern

      included do
        class_attribute :parent_association
      end

      module ClassMethods

        include Forwardable

        def belongs_to(name)
          if parent_association
            raise InvalidRecordConfiguration,
              "Can't declare more than one belongs_to association"
          end
          if table_schema.key_columns.any?
            raise InvalidRecordConfiguration,
              "belongs_to association must be declared before declaring key(s)"
          end
          self.parent_association = BelongsToAssociation.new(self, name)
          parent_association.association_key_columns.each do |column|
            key :"#{name}_#{column.name}", column.type
          end
          def_parent_association_accessors
        end

        private

        def def_parent_association_accessors
          def_parent_association_reader
          def_parent_association_writer
        end

        def def_parent_association_reader
          def_delegator 'self', :read_parent_association,
            parent_association.name
        end

        def def_parent_association_writer
          def_delegator 'self', :write_parent_association,
            "#{parent_association.name}="
        end

      end

      private

      def read_parent_association
        ivar_name = parent_association.instance_variable_name
        if instance_variable_defined?(ivar_name)
          return instance_variable_get(ivar_name)
        end
        parent_key_values = key_values.
          first(parent_association.association_key_columns.length)
        if parent_key_values.none? { |value| value.nil? }
          clazz = parent_association.association_class
          parent = parent_key_values.inject(clazz) do |record_set, key_value|
            record_set[key_value]
          end
          instance_variable_set(ivar_name, parent)
        end
      end

      def write_parent_association(parent)
        unless parent.is_a?(parent_association.association_class)
          raise ArgumentError,
            "Wrong class for #{parent_association.name}; expected " +
            "#{parent_association.association_class.name}, got " +
            "#{parent.class.name}"
        end
        instance_variable_set "@#{parent_association.name}", parent
        key_column_names = self.class.key_column_names
        parent.key_attributes.
          zip(key_column_names) do |(parent_column_name, value), column_name|
            if value.nil?
              raise ArgumentError,
                "Can't set parent association #{parent_association.name.inspect} " +
                "without value in key #{parent_column_name.inspect}"
            end
            write_attribute(column_name, value)
          end
      end

    end

  end

end
