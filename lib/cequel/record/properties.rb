module Cequel

  module Record

    module Properties

      extend ActiveSupport::Concern

      included do
        class_attribute :default_attributes, :instance_writer => false
        self.default_attributes = {}

        class <<self; alias_method :new_empty, :new; end
        extend ConstructorMethods

        attr_reader :collection_proxies
        private :collection_proxies
      end

      module ConstructorMethods

        def new(*args, &block)
          new_empty.tap do |record|
            record.__send__(:initialize_new_record, *args)
            yield record if block_given?
          end
        end

      end

      module ClassMethods

        protected

        def key(name, type, options = {})
          def_accessors(name)
          if options.fetch(:auto, false)
            unless Type[type].is_a?(Cequel::Type::Uuid)
              raise ArgumentError, ":auto option only valid for UUID columns"
            end
            default = -> { CassandraCQL::UUID.new } if options.fetch(:auto, false)
          end
          set_attribute_default(name, default)
        end

        def column(name, type, options = {})
          def_accessors(name)
          set_attribute_default(name, options[:default])
        end

        def list(name, type, options = {})
          def_collection_accessors(name, List)
          set_attribute_default(name, options.fetch(:default, []))
        end

        def set(name, type, options = {})
          def_collection_accessors(name, Set)
          set_attribute_default(name, options.fetch(:default, ::Set[]))
        end

        def map(name, key_type, value_type, options = {})
          def_collection_accessors(name, Map)
          set_attribute_default(name, options.fetch(:default, {}))
        end

        private

        def def_accessors(name)
          name = name.to_sym
          def_reader(name)
          def_writer(name)
        end

        def def_reader(name)
          module_eval <<-RUBY, __FILE__, __LINE__+1
            def #{name}; read_attribute(#{name.inspect}); end
          RUBY
        end

        def def_writer(name)
          module_eval <<-RUBY, __FILE__, __LINE__+1
            def #{name}=(value); write_attribute(#{name.inspect}, value); end
          RUBY
        end

        def def_collection_accessors(name, collection_proxy_class)
          def_collection_reader(name, collection_proxy_class)
          def_collection_writer(name)
        end

        def def_collection_reader(name, collection_proxy_class)
          module_eval <<-RUBY, __FILE__, __LINE__+1
            def #{name}
              proxy_collection(#{name.inspect}, #{collection_proxy_class})
            end
          RUBY
        end

        def def_collection_writer(name)
          module_eval <<-RUBY, __FILE__, __LINE__+1
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

      # FIXME this isn't empty anymore! Rethink.
      def initialize(attributes = {}, record_collection = nil)
        @attributes, @record_collection = attributes, record_collection
        @collection_proxies = {}
      end

      def attribute_names
        @attributes.keys
      end

      def attributes
        attribute_names.each_with_object({}) do |name, attributes|
          attributes[name] = read_attribute(name)
        end
      end

      def attributes=(attributes)
        attributes.each_pair do |attribute, value|
          __send__(:"#{attribute}=", value)
        end
      end

      def ==(other)
        if key_values.any? { |value| value.nil? }
          super
        else
          self.class == other.class && key_values == other.key_values
        end
      end

      def inspect
        inspected_attributes = attributes.each_pair.map do |attr, value|
          inspected_value = value.is_a?(CassandraCQL::UUID) ?
            value.to_guid :
            value.inspect
          "#{attr}: #{inspected_value}"
        end
        "#<#{self.class} #{inspected_attributes.join(", ")}>"
      end

      protected

      def read_attribute(name)
        @attributes.fetch(name)
      rescue KeyError
        if self.class.reflect_on_column(name)
          raise MissingAttributeError, "missing attribute: #{name}"
        else
          raise UnknownAttributeError, "unknown attribute: #{name}"
        end
      end

      def write_attribute(name, value)
        @attributes[name] = value
      end

      private

      def proxy_collection(column_name, proxy_class)
        column = self.class.reflect_on_column(column_name)
        collection_proxies[column_name] ||= proxy_class.new(self, column)
      end

      def reset_collection_proxy(name)
        collection_proxies.delete(name)
      end

      def initialize_new_record(attributes = {})
        dynamic_defaults = default_attributes.
          select { |name, value| value.is_a?(Proc) }
        @attributes = Marshal.load(Marshal.dump(
          default_attributes.except(*dynamic_defaults.keys)))
          dynamic_defaults.each { |name, p| @attributes[name] = p.() }
          @new_record = true
          yield self if block_given?
          self.attributes = attributes
          loaded!
          self
      end

    end

  end

end
