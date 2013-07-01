module Cequel

  module Model

    module Properties

      extend ActiveSupport::Concern

      module ClassMethods

        protected

        def key(name, type)
          def_accessors(name)
          schema.add_key(name, type)
          set_attribute_default(name, nil)
        end

        def column(name, type, options = {})
          def_accessors(name)
          schema.add_data_column(name, type, options[:index])
          set_attribute_default(name, options[:default])
        end

        def list(name, type, options = {})
          def_accessors(name)
          schema.add_list(name, type)
          set_attribute_default(name, options.fetch(:default, []))
        end

        def set(name, type, options = {})
          def_accessors(name)
          schema.add_set(name, type)
          set_attribute_default(name, options.fetch(:default, Set[]))
        end

        def map(name, key_type, value_type, options = {})
          def_accessors(name)
          schema.add_map(name, key_type, value_type)
          set_attribute_default(name, options.fetch(:default, {}))
        end

        def table_property(name, value)
          schema.add_property(name, value)
        end

        private

        def def_accessors(name)
          name = name.to_sym
          module_eval <<-RUBY
            def #{name}; read_attribute(#{name.inspect}); end
            def #{name}=(value); write_attribute(#{name.inspect}, value); end
          RUBY
        end

        def set_attribute_default(name, default)
          default_attributes[name.to_sym] = default
        end

      end

      protected
      delegate :schema, :to => 'self.class'

      def read_attribute(name)
        attributes.fetch(name)
      rescue KeyError
        if schema.column(name)
          raise MissingAttributeError, "missing attribute: #{name}"
        else
          raise UnknownAttributeError, "unknown attribute: #{name}"
        end
      end

      def write_attribute(name, value)
        column = schema.column(name)
        raise UnknownAttributeError,
          "unknown attribute: #{name}" unless column
        attributes[name] = value.nil? ? nil : column.cast(value)
      end

    end

  end

end
