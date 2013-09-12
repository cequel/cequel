module Cequel

  module Model

    module Properties

      extend ActiveSupport::Concern

      module ClassMethods

        protected

        def key(name, type)
          def_accessors(name)
          table_schema.add_key(name, type)
          set_attribute_default(name, nil)
        end

        def column(name, type, options = {})
          def_accessors(name)
          table_schema.add_data_column(name, type, options[:index])
          set_attribute_default(name, options[:default])
        end

        def list(name, type, options = {})
          def_collection_accessors(name, List)
          table_schema.add_list(name, type)
          set_attribute_default(name, options.fetch(:default, []))
        end

        def set(name, type, options = {})
          def_collection_accessors(name, Set)
          table_schema.add_set(name, type)
          set_attribute_default(name, options.fetch(:default, ::Set[]))
        end

        def map(name, key_type, value_type, options = {})
          def_collection_accessors(name, Map)
          table_schema.add_map(name, key_type, value_type)
          set_attribute_default(name, options.fetch(:default, {}))
        end

        def table_property(name, value)
          table_schema.add_property(name, value)
        end

        private

        def def_accessors(name)
          name = name.to_sym
          def_reader(name)
          def_writer(name)
        end

        def def_reader(name)
          module_eval <<-RUBY
            def #{name}; read_attribute(#{name.inspect}); end
          RUBY
        end

        def def_writer(name)
          module_eval <<-RUBY
            def #{name}=(value); write_attribute(#{name.inspect}, value); end
          RUBY
        end

        def def_collection_accessors(name, collection_proxy_class)
          def_collection_reader(name, collection_proxy_class)
          def_collection_writer(name)
        end

        def def_collection_reader(name, collection_proxy_class)
          module_eval <<-RUBY
            def #{name}
              proxy_collection(#{name.inspect}, #{collection_proxy_class})
            end
          RUBY
        end

        def def_collection_writer(name)
          module_eval <<-RUBY
            def #{name}=(value)
              reset_collection_proxy(#{name.inspect})
              write_attribute(#{name.inspect}, value)
            end
          RUBY
        end

        def set_attribute_default(name, default)
          default_attributes[name.to_sym] = default
        end

      end

      def attributes=(attributes)
        attributes.each_pair do |attribute, value|
          __send__(:"#{attribute}=", value)
        end
      end

      protected
      delegate :table_schema, :to => 'self.class'

      def read_attribute(name)
        attributes.fetch(name)
      rescue KeyError
        if table_schema.column(name)
          raise MissingAttributeError, "missing attribute: #{name}"
        else
          raise UnknownAttributeError, "unknown attribute: #{name}"
        end
      end

      def write_attribute(name, value)
        column = table_schema.column(name)
        raise UnknownAttributeError,
          "unknown attribute: #{name}" unless column
        attributes[name] = value.nil? ? nil : column.cast(value)
      end

      private

      def proxy_collection(name, proxy_class)
        collection_proxies[name] ||= proxy_class.new(self, name)
      end

      def reset_collection_proxy(name)
        collection_proxies.delete(name)
      end

    end

  end

end
