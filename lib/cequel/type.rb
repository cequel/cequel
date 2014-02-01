require 'singleton'

module Cequel
  #
  # The Type module encapsulates information about the CQL3 type system. Each
  # type has a `cql_name`, which is the name of the type as defined in CQL, and
  # an `internal_name`, which is the name of the type in the lower-level
  # interface that is exposed when introspecting table information in the
  # database.
  #
  # As well as knowing their respective names, types also know how to cast Ruby
  # objects to the correct canonical class corresponding to the type. These
  # implicit types are used by the underlying `cassandra-cql` library to
  # determine how to represent values when passing them to Cassandra.
  #
  # @since 1.0.0
  #
  module Type
    # Raised if an unknown type is looked up
    UnknownType = Class.new(ArgumentError)

    BY_CQL_NAME = {}
    BY_INTERNAL_NAME = {}

    #
    # Register a type for lookup
    #
    # @param type [Type] a new type
    # @return [void]
    #
    def self.register(type)
      BY_CQL_NAME[type.cql_name] = type
      type.cql_aliases.each { |aliaz| BY_CQL_NAME[aliaz] = type }
      type.internal_names.each do |internal_name|
        BY_INTERNAL_NAME[internal_name] = type
      end
    end

    #
    # Return a type corresponding to the given input
    #
    # @param cql_name [Symbol,Base] CQL name of a type, or a type
    # @return [Base] type with the given CQL name
    #
    def self.[](cql_name)
      cql_name.is_a?(Base) ? cql_name : lookup_cql(cql_name)
    end

    #
    # Look up a type by CQL name
    #
    # @param cql_name [Symbol] CQL name of a type
    # @return [Base] type with the given CQL name
    # @raise [UnknownType] if no type by that name is registered
    #
    def self.lookup_cql(cql_name)
      BY_CQL_NAME.fetch(cql_name.to_sym)
    rescue KeyError
      raise UnknownType, "Unrecognized CQL type #{cql_name.inspect}"
    end

    #
    # Look up a type by internal name
    #
    # @param internal_name [String] internal name of a type
    # @return [Base] type with the given internal name
    # @raise [UnknownType] if no type by that name is registered
    #
    def self.lookup_internal(internal_name)
      BY_INTERNAL_NAME.fetch(internal_name)
    rescue KeyError
      raise UnknownType, "Unrecognized internal type #{internal_name.inspect}"
    end

    #
    # The base class for all type objects. Types are singletons.
    #
    # @abstract Subclasses should implement {#cast}, and may implement
    #   {#internal_names} if it cannot be inferred from the class name. The
    #   name of the type class should be the camel-cased CQL name of the type
    #
    class Base
      include Singleton

      #
      # @return the name of the type used in CQL. This is also the name that is
      #   used in all of Cequel's public interfaces
      #
      def cql_name
        self.class.name.demodulize.underscore.to_sym
      end

      #
      # @return [Array<Symbol>] other names used in CQL for this type
      #
      def cql_aliases
        []
      end

      #
      # @return [Array<String>] full class name of this type used in
      #   Cassandra's underlying representation
      #
      # @deprecated use {internal_names}
      #
      def internal_name
        internal_names.first
      end

      #
      # @return [Array<String>] full class name(s) of this type used in
      #   Cassandra's underlying representation (allows for multiple values for
      #   types that have different names between different versions)
      #
      def internal_names
        ["org.apache.cassandra.db.marshal.#{self.class.name.demodulize}Type"]
      end

      #
      # @param value the value to cast
      # @return the value cast to the correct Ruby class for this type
      #
      def cast(value)
        value
      end

      #
      # CQL only allows changing column types when the old type's binary
      # representation is compatible with the new type.
      #
      # @return [Array<Type>] new types that columns of this type may be
      #   altered to
      #
      def compatible_types
        [Type[:blob]]
      end

      #
      # A string representation of this type
      #
      def to_s
        cql_name.to_s
      end
    end

    #
    # Abstract superclass for types that represent character data
    #
    # @abstract Subclasses must implement `#encoding`, which returns the name
    #   of the Ruby encoding corresponding to the character encoding used for
    #   values of this type
    #
    class String < Base
      def cast(value)
        str = String(value)
        str.encoding.name == encoding ? str : str.dup.force_encoding(encoding)
      end
    end

    #
    # `ascii` columns store 7-bit ASCII character data
    #
    # @see TK CQL3 documentation for ascii type
    #
    class Ascii < String
      def compatible_types
        super + [Type[:text]]
      end

      private

      def encoding
        'US-ASCII'
      end
    end
    register Ascii.instance

    #
    # `blob` columns store arbitrary bytes of data, represented as 8-bit ASCII
    # strings of hex digits
    #
    # @see TK CQL3 documentation for blob type
    #
    class Blob < String
      def internal_names
        ['org.apache.cassandra.db.marshal.BytesType']
      end

      def cast(value)
        value = value.to_s(16) if value.is_a?(Integer)
        super
      end

      private

      def encoding
        'ASCII-8BIT'
      end
    end
    register Blob.instance

    #
    # `boolean` types store boolean values
    #
    # @see TK CQL3 documentation for boolean type
    #
    class Boolean < Base
      def cast(value)
        !!value
      end
    end
    register Boolean.instance

    #
    # Counter columns are a special type of column in Cassandra that can be
    # incremented and decremented atomically. Counter columns cannot comingle
    # with regular data columns in the same table. Unlike other columns,
    # counter columns cannot be updated without Cassandra internally reading
    # the existing state of the column
    #
    # @see TK CQL3 documentation for counter columns
    #
    class Counter < Base
      def internal_names
        ['org.apache.cassandra.db.marshal.CounterColumnType']
      end

      def compatible_types
        []
      end

      def cast(value)
        Integer(value)
      end
    end
    register Counter.instance

    #
    # `decimal` columns store decimal numeric values
    #
    # @see TK CQL3 documentation for decimal columns
    #
    class Decimal < Base
      def cast(value)
        value.is_a?(BigDecimal) ? value : BigDecimal.new(value, 0)
      end
    end
    register Decimal.instance

    #
    # `double` columns store 64-bit floating-point numeric values
    #
    # @see TK CQL3 documentation for double columns
    #
    class Double < Base
      def cast(value)
        Float(value)
      end
    end
    register Double.instance

    #
    # TK
    #
    # @see TK CQL3 documentation for inet columns
    #
    class Inet < Base
      def internal_names
        ['org.apache.cassandra.db.marshal.InetAddressType']
      end
    end
    register Inet.instance

    #
    # `int` columns store 32-bit integer values
    #
    # @see TK CQL3 documentation for int columns
    #
    class Int < Base
      def internal_names
        ['org.apache.cassandra.db.marshal.Int32Type']
      end

      def cast(value)
        Integer(value)
      end
    end
    register Int.instance

    #
    # `float` columns store 32-bit floating-point numeric values
    #
    # @see TK CQL3 documentation for float columns
    #
    class Float < Double; end
    register Float.instance

    #
    # `bigint` columns store 64-bit integer values
    #
    # @see TK CQL3 documentation for bigint columns
    #
    class Bigint < Int
      def internal_names
        ['org.apache.cassandra.db.marshal.LongType']
      end
    end
    register Bigint.instance

    #
    # `text` columns store UTF-8 character data. They are also known as
    # `varchar` columns; the names can be used interchangeably. Text columns do
    # not have a length limit
    #
    # @see TK CQL3 documentation for text columns
    #
    class Text < String
      def internal_names
        ['org.apache.cassandra.db.marshal.UTF8Type']
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

    #
    # `timestamp` columns store timestamps. Timestamps do not include time zone
    # data, and all input times are cast to UTC before being stored.
    #
    class Timestamp < Base
      def internal_names
        ['org.apache.cassandra.db.marshal.DateType',
         'org.apache.cassandra.db.marshal.TimestampType']
      end

      def cast(value)
        if value.is_a?(::String) then Time.parse(value)
        elsif value.respond_to?(:to_time) then value.to_time
        elsif value.is_a?(Numeric) then Time.at(value)
        else Time.parse(value.to_s)
        end.utc
      end
    end
    register Timestamp.instance

    #
    # `uuid` columns store type 1 and type 4 UUIDs. Cequel uses the
    # `CassandraCQL::UUID` type to represent UUIDs in Ruby, since this is what
    # the underlying `cassandra-cql` library expects. Other UUID formats are
    # supported as inputs.
    #
    class Uuid < Base
      def internal_names
        ['org.apache.cassandra.db.marshal.UUIDType']
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

    #
    # `timeuuid` columns are a special type of UUID column that support
    # time-based queries. For instance, a `timeuuid` clustering column can be
    # filtered by ranges of times into which the UUIDs must fall. This
    # functionality presumes the use of type 1 UUIDs, which encode the
    # timestamp of their creation.
    #
    class Timeuuid < Uuid
      def internal_names
        ['org.apache.cassandra.db.marshal.TimeUUIDType']
      end
    end
    register Timeuuid.instance

    #
    # `varint` columns store arbitrary-length integer data
    #
    class Varint < Int
      def internal_name
        'org.apache.cassandra.db.marshal.IntegerType'
      end
    end
    register Varint.instance
  end
end
