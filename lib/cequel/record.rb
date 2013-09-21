require 'active_model'

require 'cequel'
require 'cequel/record/errors'
require 'cequel/record/schema'
require 'cequel/record/properties'
require 'cequel/record/collection'
require 'cequel/record/persistence'
require 'cequel/record/record_set'
require 'cequel/record/scoped'
require 'cequel/record/secondary_indexes'
require 'cequel/record/associations'
require 'cequel/record/association_collection'
require 'cequel/record/belongs_to_association'
require 'cequel/record/has_many_association'
require 'cequel/record/mass_assignment'
require 'cequel/record/callbacks'
require 'cequel/record/validations'
require 'cequel/record/dirty'

require 'cequel/record'

if defined? Rails
  require 'cequel/record/railtie'
end

module Cequel

  module Record

    extend ActiveSupport::Concern
    extend Forwardable

    included do
      include Properties
      include Schema
      include Persistence
      include Associations
      extend Scoped
      extend SecondaryIndexes
      include MassAssignment
      include Callbacks
      include Validations
      include Dirty
      extend ActiveModel::Naming
      include ActiveModel::Serializers::JSON
      include ActiveModel::Serializers::Xml

      class_attribute :table_name, :default_attributes,
        :instance_writer => false

      self.default_attributes = {}
      self.table_name = name.tableize.to_sym unless name.nil?

      class <<self; alias_method :new_empty, :new; end
      extend ConstructorMethods
    end

    class <<self
      attr_accessor :connection

      def establish_connection(configuration)
        self.connection = Cequel.connect(configuration)
      end
    end

    module ClassMethods
      extend Forwardable
      def_delegator 'Cequel::Record', :connection
    end

    module ConstructorMethods

      def new(*args, &block)
        new_empty.tap do |record|
          record.__send__(:initialize_new_record, *args)
          yield record if block_given?
        end
      end

    end

    def_delegator 'self.class', :connection

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
