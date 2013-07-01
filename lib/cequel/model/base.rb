require 'cequel/model/schema'
require 'cequel/model/properties'

module Cequel

  module Model

    class Base

      include Cequel::Model::Properties
      include Cequel::Model::Schema

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

      def initialize
        @attributes = Marshal.load(Marshal.dump(default_attributes))
        yield self if block_given?
      end

    end

  end

end
