# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # Properties on a Cequel record acts as attributes on record instances, and
    # are persisted as column values to Cassandra. Properties are declared
    # explicitly on a record instance in the body.
    #
    # Properties can be **key columns**, **data columns**, or **collection
    # columns**. Key columns combine to form the primary key for the record;
    # they cannot be changed once a record has been saved. Data columns contain
    # scalar data values like strings, integers, and timestamps. Collection
    # columns are lists, sets, or maps that can be atomically updated.
    #
    # All varieties of column have a type; see {Cequel::Type} for the full
    # list of possibilities. A collection column's type is the type of its
    # elements (in the case of a map collection, there is both a key type and a
    # value type).
    #
    # @example
    #   class Post
    #     key :blog_subdomain, :text
    #     key :id, :timeuuid, auto: true
    #
    #     column :title, :text
    #     column :body, :text
    #     column :updated_at, :timestamp
    #
    #     list :categories, :text
    #     set :tags, :text
    #     map :referers, :text, :integer
    #   end
    #
    # @see ClassMethods Methods for defining properties
    #
    module Properties
      extend ActiveSupport::Concern

      included do
        class_attribute :default_attributes, instance_writer: false
        class_attribute :empty_attributes, instance_writer: false
        self.default_attributes, self.empty_attributes = {}, {}

        class <<self; alias_method :new_empty, :new; end
        extend ConstructorMethods

        attr_reader :collection_proxies
        private :collection_proxies
      end

      # @private
      module ConstructorMethods
        def new(*args, &block)
          new_empty.tap do |record|
            record.__send__(:initialize_new_record, *args)
            yield record if block_given?
          end
        end
      end

      #
      # Methods for defining columns on a record
      #
      # @see Properties
      #
      module ClassMethods
        protected

        # rubocop:disable LineLength

        # @!visibility public

        #
        # Define a key column. By default, the first key column defined for a
        # record will be a partition key, and the following keys will be
        # clustering columns. This behavior can be changed using the
        # `:partition` option
        #
        # @param name [Symbol] the name of the key column
        # @param type [Symbol] the type of the key column
        # @param options [Options] options for the key column
        # @option options [Boolean] :partition (false) make this a partition
        #   key even if it is not the first key column
        # @option options [Boolean] :auto (false) automatically initialize this
        #   key with a UUID value for new records. Only valid for `uuid` and
        #   `timeuuid` columns.
        # @option options [:asc,:desc] :order whether rows should be ordered
        #   ascending or descending by this column. Only valid for clustering
        #   columns
        # @return [void]
        #
        # @note {Associations::ClassMethods#belongs_to belongs_to} implicitly
        #   defines key columns.
        #
        # @see
        #   http://cassandra.apache.org/doc/cql3/CQL.html#createTablepartitionClustering
        #   CQL documentation on compound primary keys
        #
        def key(name, type, options = {})
          def_accessors(name)
          if options.fetch(:auto, false)
            unless Type[type].is_a?(Cequel::Type::Uuid)
              fail ArgumentError, ":auto option only valid for UUID columns"
            end
            default = -> { Cequel.uuid } if options[:auto]
          else
            default = options[:default]
          end
          set_attribute_default(name, default)
        end

        # rubocop:enable LineLength

        #
        # Define a data column
        #
        # @param name [Symbol] the name of the column
        # @param type [Symbol] the type of the column
        # @param options [Options] options for the column
        # @option options [Object,Proc] :default a default value for the
        #   column, or a proc that returns a default value for the column
        # @option options [Boolean,Symbol] :index create a secondary index on
        #   this column
        # @return [void]
        #
        # @note Secondary indexes are not nearly as flexible as primary keys:
        #   you cannot query for multiple values or for ranges of values. You
        #   also cannot combine a secondary index restriction with a primary
        #   key restriction in the same query, nor can you combine more than
        #   one secondary index restriction in the same query.
        #
        def column(name, type, options = {})
          def_accessors(name)
          set_attribute_default(name, options[:default])
        end

        #
        # Define a list column
        #
        # @param name [Symbol] the name of the list
        # @param type [Symbol] the type of the elements in the list
        # @param options [Options] options for the list
        # @option options [Object,Proc] :default ([]) a default value for the
        #   column, or a proc that returns a default value for the column
        # @return [void]
        #
        # @see Record::List
        # @since 1.0.0
        #
        def list(name, type, options = {})
          def_collection_accessors(name, List)
          set_attribute_default(name, options[:default])
          set_empty_attribute(name) { [] }
        end

        #
        # Define a set column
        #
        # @param name [Symbol] the name of the set
        # @param type [Symbol] the type of the elements in the set
        # @param options [Options] options for the set
        # @option options [Object,Proc] :default (Set[]) a default value for
        #   the column, or a proc that returns a default value for the column
        # @return [void]
        #
        # @see Record::Set
        # @since 1.0.0
        #
        def set(name, type, options = {})
          def_collection_accessors(name, Set)
          set_attribute_default(name, options[:default])
          set_empty_attribute(name) { ::Set[] }
        end

        #
        # Define a map column
        #
        # @param name [Symbol] the name of the map
        # @param key_type [Symbol] the type of the keys in the set
        # @param options [Options] options for the set
        # @option options [Object,Proc] :default ({}) a default value for the
        #   column, or a proc that returns a default value for the column
        # @return [void]
        #
        # @see Record::Map
        # @since 1.0.0
        #
        def map(name, key_type, value_type, options = {})
          def_collection_accessors(name, Map)
          set_attribute_default(name, options[:default])
          set_empty_attribute(name) { {} }
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

        def set_empty_attribute(name, &block)
          empty_attributes[name.to_sym] = block
        end
      end

      # @private
      def initialize(attributes = {}, record_collection = nil)
        @attributes, @record_collection = attributes, record_collection
        @collection_proxies = {}
      end

      #
      # @return [Array<Symbol>] list of names of attributes on this record
      #
      def attribute_names
        @attributes.keys
      end

      #
      # @return [Hash<String,Object>] map of column names to values currently
      #   set on this record
      #
      def attributes
        attribute_names
          .each_with_object(HashWithIndifferentAccess.new) do |name, attributes|
          attributes[name] = read_attribute(name)
        end
      end

      #
      # Set attributes on the record. Each attribute is set via the setter
      # method; virtual (non-column) attributes are allowed.
      #
      # @param attributes [Hash] map of attribute names to values
      # @return [void]
      #
      def attributes=(attributes)
        attributes.each_pair do |attribute, value|
          __send__(:"#{attribute}=", value)
        end
      end

      #
      # Read an attribute
      #
      # @param column_name [Symbol] the name of the column
      # @return the value of that column
      # @raise [MissingAttributeError] if the attribute has not been loaded
      # @raise [UnknownAttributeError] if the attribute does not exist
      #
      def [](column_name)
        read_attribute(column_name)
      end

      #
      # Write an attribute
      #
      # @param column_name [Symbol] name of the column to write
      # @param value the value to write to the column
      # @return [void]
      # @raise [UnknownAttributeError] if the attribute does not exist
      #
      def []=(column_name, value)
        write_attribute(column_name, value)
      end

      #
      # @return [Boolean] true if this record has the same type and key
      #   attributes as the other record
      def ==(other)
        if key_values.any? { |value| value.nil? }
          super
        else
          self.class == other.class && key_values == other.key_values
        end
      end

      #
      # @return [String] string representation of the record
      #
      def inspect
        inspected_attributes = attributes.each_pair.map do |attr, value|
          inspected_value = Cequel.uuid?(value) ?
            value.to_s :
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
          fail MissingAttributeError, "missing attribute: #{name}"
        else
          fail UnknownAttributeError, "unknown attribute: #{name}"
        end
      end

      def write_attribute(name, value)
        unless self.class.reflect_on_column(name)
          fail UnknownAttributeError, "unknown attribute: #{name}"
        end
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

      def init_attributes(new_attributes)
        @attributes = {}
        new_attributes.each_pair do |name, value|
          if value.nil?
            value = empty_attributes.fetch(name.to_sym) { -> {} }.call
          end
          @attributes[name.to_sym] = value
        end
        @attributes
      end

      def initialize_new_record(attributes = {})
        dynamic_defaults = default_attributes
          .select { |name, value| value.is_a?(Proc) }
        new_attributes =
          Util.deep_copy(default_attributes.except(*dynamic_defaults.keys))
        dynamic_defaults.each { |name, p| new_attributes[name] = p.call }
        init_attributes(new_attributes)

        @new_record = true
        yield self if block_given?
        self.attributes = attributes
        loaded!
        self
      end
    end
  end
end
