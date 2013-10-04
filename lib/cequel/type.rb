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
      cql_name.is_a?(Base) ? cql_name : lookup_cql(cql_name)
    end

    def self.lookup_cql(cql_name)
      BY_CQL_NAME.fetch(cql_name.to_sym)
    rescue KeyError
      raise UnknownType, "Unrecognized CQL type #{cql_name.inspect}"
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

      def cast(value)
        value
      end

      def to_s
        cql_name.to_s
      end

    end

    class String < Base

      def cast(value)
        str = String(value)
        str.encoding.name == encoding ? str : str.dup.force_encoding(encoding)
      end

    end

    class Ascii < String
      private

      def encoding
        'US-ASCII'
      end
    end
    register Ascii.instance

    class Blob < String

      def internal_name
        'org.apache.cassandra.db.marshal.BytesType'
      end

      def cast(value)
        value = value.to_s(16) if Integer === value
        super
      end

      private

      def encoding
        'ASCII-8BIT'
      end

    end
    register Blob.instance

    class Boolean < Base
      def cast(value)
        !!value
      end
    end
    register Boolean.instance

    class Counter < Base

      def internal_name
        'org.apache.cassandra.db.marshal.CounterColumnType'
      end

      def cast(value)
        Integer(value)
      end

    end
    register Counter.instance

    class Decimal < Base
      def cast(value)
        BigDecimal === value ? value : BigDecimal.new(value, 0)
      end
    end
    register Decimal.instance

    class Double < Base
      def cast(value)
        Float(value)
      end
    end
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

      def cast(value)
        Integer(value)
      end

    end
    register Int.instance

    class Float < Double; end
    register Float.instance

    class Long < Int

      def internal_name
        'org.apache.cassandra.db.marshal.LongType'
      end

    end
    register Long.instance

    class Text < String

      def internal_name
        'org.apache.cassandra.db.marshal.UTF8Type'
      end

      def cql_aliases
        [:varchar]
      end

      private

      def encoding
        'UTF-8'
      end

    end
    register Text.instance

    class Timestamp < Base

      def internal_name
        'org.apache.cassandra.db.marshal.DateType'
      end

      def cast(value)
        if ::String === value then Time.parse(value)
        elsif value.respond_to?(:to_time) then value.to_time
        elsif Numeric === value then Time.at(value)
        else Time.parse(value.to_s)
        end.utc
      end

    end
    register Timestamp.instance

    class Uuid < Base

      def internal_name
        'org.apache.cassandra.db.marshal.UUIDType'
      end

      def cast(value)
        case value
        when CassandraCQL::UUID then value
        when SimpleUUID::UUID then CassandraCQL::UUID.new(value.to_s)
        else CassandraCQL::UUID.new(value)
        end
      end

    end
    register Uuid.instance

    class Timeuuid < Uuid

      def internal_name
        'org.apache.cassandra.db.marshal.TimeUUIDType'
      end

    end
    register Timeuuid.instance

    class Varint < Int

      def internal_name
        'org.apache.cassandra.db.marshal.IntegerType'
      end

    end
    register Varint.instance

  end

end
