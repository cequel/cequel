module Cequel

  module Model

    class Base

      include Cequel::Model::Properties
      include Cequel::Model::Schema
      include Cequel::Model::Persistence
      include Cequel::Model::Associations
      extend Cequel::Model::Scoped
      extend Cequel::Model::SecondaryIndexes
      include Cequel::Model::MassAssignment
      include Cequel::Model::Callbacks
      include Cequel::Model::Validations
      include Cequel::Model::Dirty
      extend ActiveModel::Naming
      include ActiveModel::Serializers::JSON
      include ActiveModel::Serializers::Xml

      class_attribute :table_name, :connection, :default_attributes,
        :instance_writer => false

      def self.inherited(base)
        base.table_name = base.name.tableize.to_sym unless base.name.nil?
        base.default_attributes = {}
      end

      def self.establish_connection(configuration)
        self.connection = Cequel.connect(configuration)
      end

      class <<self; alias_method :new_empty, :new; end
      def self.new(*args, &block)
        new_empty.tap do |record|
          record.__send__(:initialize_new_record, *args)
          yield record if block_given?
        end
      end

      def initialize(&block)
        @attributes, @collection_proxies = {}, {}
        instance_eval(&block) if block
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
      attr_reader :collection_proxies

      private

      def initialize_new_record(attributes = {})
        dynamic_defaults = default_attributes.
          select { |name, value| value.is_a?(Proc) }
        @attributes = Marshal.load(Marshal.dump(
          default_attributes.except(*dynamic_defaults.keys)))
        dynamic_defaults.each { |name, p| @attributes[name] = p.() }
        @new_record = true
        yield self if block_given?
        self.attributes = attributes #XXX this should really be in Properties
        loaded!
        self
      end

    end

  end

  Base = Model::Base

end
