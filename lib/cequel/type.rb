require 'singleton'

module Cequel

  module Type

    UnknownType = Class.new(ArgumentError)

    BY_CQL_NAME = {}
    BY_INTERNAL_NAME = {}

    def self.register(type)
      BY_CQL_NAME[type.cql_name] = type
      BY_INTERNAL_NAME[type.internal_name] = type
    end

    def self.[](cql_name)
      BY_CQL_NAME.fetch(cql_name.to_sym)
    end

    def self.lookup_internal(internal_name)
      BY_INTERNAL_NAME.fetch(internal_name)
    rescue KeyError
      raise UnknownType, "Unrecognized internal type #{internal_name.inspect}"
    end

    class Base
      include Singleton

      def cql_name
        self.class.name.demodulize.underscore.to_sym
      end

      def internal_name
        "org.apache.cassandra.db.marshal.#{self.class.name.demodulize}Type"
      end

      def to_s
        cql_name.to_s
      end

    end

    class Ascii < Base; end
    register Ascii.instance

    class Blob < Base

      def internal_name
        'org.apache.cassandra.db.marshal.BytesType'
      end

    end
    register Blob.instance

    class Text < Base

      def internal_name
        'org.apache.cassandra.db.marshal.UTF8Type'
      end

    end
    register Text.instance

    class Timestamp < Base

      def internal_name
        'org.apache.cassandra.db.marshal.DateType'
      end

    end
    register Timestamp.instance

    class Uuid < Base

      def internal_name
        'org.apache.cassandra.db.marshal.UUIDType'
      end

    end
    register Uuid.instance

  end

end
