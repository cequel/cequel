module Cequel

  module Model

    class Base

      include Cequel::Model::Properties
      include Cequel::Model::Schema
      include Cequel::Model::Persistence
      include Cequel::Model::Associations
      extend Cequel::Model::Scoped
      extend ActiveModel::Naming

      include Cequel::Model::Callbacks
      include Cequel::Model::Validations

      class_attribute :table_name, :connection, :default_attributes,
        :instance_writer => false
      attr_reader :attributes

      def self.inherited(base)
        base.table_name = name.tableize.to_sym
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

      protected
      attr_reader :collection_proxies

      private

      def initialize_new_record(*args)
        @attributes = Marshal.load(Marshal.dump(default_attributes))
        @new_record = true
        yield self if block_given?
        if Hash === args.first
          args.first.each_pair do |k, v|
            self.send("#{k}=", v)
          end
        end
        loaded!
        self
      end

    end

  end

  Base = Model::Base

end
