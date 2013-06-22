require 'cequel/model/schema'
require 'cequel/model/properties'

module Cequel

  module Model

    class Base

      include Cequel::Model::Properties
      include Cequel::Model::Schema

      class_attribute :table_name, :connection, :instance_writer => false

      def self.inherited(base)
        base.table_name = name.underscore.to_sym
      end

      def self.establish_connection(configuration)
        self.connection = Cequel.connect(configuration)
      end

    end

  end

end
