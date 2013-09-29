module Cequel

  module Record

    module Associations

      extend ActiveSupport::Concern

      included do
        class_attribute :parent_association
        class_attribute :child_associations
        self.child_associations = {}
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
          self.parent_association = BelongsToAssociation.new(self, name.to_sym)
          parent_association.association_key_columns.each do |column|
            key :"#{name}_#{column.name}", column.type
          end
          def_parent_association_accessors
        end

        def has_many(name, options = {})
          options.assert_valid_keys(:dependent)

          association = HasManyAssociation.new(self, name.to_sym)
          self.child_associations =
            child_associations.merge(name => association)
          def_child_association_reader(association)

          case options[:dependent]
          when :destroy
            after_destroy { delete_children(name, true) }
          when :delete
            after_destroy { delete_children(name) }
          when nil
          else
            raise ArgumentError, "Invalid option #{options[:dependent].inspect} provided for :dependent. Specify :destroy or :delete."
          end
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

        def def_child_association_reader(association)
          module_eval <<-RUBY, __FILE__, __LINE__+1
            def #{association.name}(reload = false)
              read_child_association(#{association.name.inspect}, reload)
            end
          RUBY
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

      def read_child_association(association_name, reload = false)
        association = child_associations[association_name]
        ivar = association.instance_variable_name
        if !reload && instance_variable_defined?(ivar)
          return instance_variable_get(ivar)
        end
        association_record_set = key_values.inject(association.association_class) do |record_set, key_value|
          record_set[key_value]
        end
        instance_variable_set(
          ivar, AssociationCollection.new(association_record_set))
      end

      def delete_children(association_name, run_callbacks = false)
        if run_callbacks
          self.send(association_name).each do |c|
            c.run_callbacks(:destroy)
          end
        end
        connection[association_name].where(
          send(association_name).scoped_key_attributes
        ).delete
      end

    end

  end

end
