require 'cequel/model/schema'
require 'cequel/model/properties'
require 'cequel/model/collection'
require 'cequel/model/persistence'

module Cequel

  module Model

    class Base

      include Cequel::Model::Properties
      include Cequel::Model::Schema
      include Cequel::Model::Persistence

      class_attribute :table_name, :connection, :default_attributes,
        :instance_writer => false
      attr_reader :attributes

      def self.inherited(base)
        base.table_name = name.underscore.to_sym
        base.default_attributes = {}
      end

      def self.establish_connection(configuration)
        self.connection = Cequel.connect(configuration)
      end

      def self.new_empty(&block)
        allocate.tap do |instance|
          instance.initialize_empty
          instance.instance_eval(&block) if block
        end
      end

      def initialize
        initialize_empty
        @attributes = Marshal.load(Marshal.dump(default_attributes))
        @new_record = true
        yield self if block_given?
      end

      def initialize_empty
        @attributes, @collection_proxies = {}, {}
      end

      protected
      attr_reader :collection_proxies

    end

  end

  Base = Model::Base

end
