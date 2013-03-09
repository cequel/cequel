require 'singleton'

module Cequel

  module Type

    UnknownType = Class.new(ArgumentError)

    BY_CQL_NAME = {}
    BY_INTERNAL_NAME = {}

    def self.register(type)
      BY_CQL_NAME[type.cql_name] = type
      type.cql_aliases.each { |aliaz| BY_CQL_NAME[aliaz] = type }
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

      def cql_aliases
        []
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

    class Boolean < Base; end
    register Boolean.instance

    class Counter < Base

      def internal_name
        'org.apache.cassandra.db.marshal.CounterColumnType'
      end

    end
    register Counter.instance

    class Decimal < Base; end
    register Decimal.instance

    class Double < Base; end
    register Double.instance

    class Inet < Base

      def internal_name
        'org.apache.cassandra.db.marshal.InetAddressType'
      end

    end
    register Inet.instance

    class Int < Base

      def internal_name
        'org.apache.cassandra.db.marshal.Int32Type'
      end

    end
    register Int.instance

    class Float < Base; end
    register Float.instance

    class Long < Base; end
    register Long.instance

    class Text < Base

      def internal_name
        'org.apache.cassandra.db.marshal.UTF8Type'
      end

      def cql_aliases
        [:varchar]
      end

    end
    register Text.instance

    class Timestamp < Base

      def internal_name
        'org.apache.cassandra.db.marshal.DateType'
      end

    end
    register Timestamp.instance

    class Timeuuid < Base

      def internal_name
        'org.apache.cassandra.db.marshal.TimeUUIDType'
      end

    end
    register Timeuuid.instance

    class Uuid < Base

      def internal_name
        'org.apache.cassandra.db.marshal.UUIDType'
      end

    end
    register Uuid.instance

    class Varint < Base

      def internal_name
        'org.apache.cassandra.db.marshal.IntegerType'
      end

    end
    register Varint.instance

  end

end
