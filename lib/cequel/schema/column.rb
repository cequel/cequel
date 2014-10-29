# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # Represents a column definition in a table schema.
    #
    # @abstract
    #
    class Column
      # @return [Symbol] the name of the column
      attr_reader :name
      # @return [Type] the type of the column
      attr_reader :type

      #
      # @param name [Symbol] the name of the column
      # @param type [Type] the type of the column
      #
      def initialize(name, type)
        @name, @type = name, type
      end

      # rubocop:disable LineLength

      #
      # @return [Boolean] true if this is a key column
      #
      # @see
      #   http://cassandra.apache.org/doc/cql3/CQL.html#createTablepartitionClustering
      #   CQL3 key documentation
      #
      def key?
        partition_key? || clustering_column?
      end

      #
      # @return [Boolean] true if this is a partition key column
      #
      # @see
      #   http://cassandra.apache.org/doc/cql3/CQL.html#createTablepartitionClustering
      #   CQL3 key documentation
      #
      def partition_key?
        false
      end

      #
      # @return [Boolean] true if this is a clustering column
      #
      # @see
      #   http://cassandra.apache.org/doc/cql3/CQL.html#createTablepartitionClustering
      #   CQL3 key documentation
      #
      def clustering_column?
        false
      end

      # rubocop:enable LineLength

      #
      # @return [Boolean] true if this is a data column
      #
      def data_column?
        !key?
      end

      #
      # @return [Boolean] true if this is a collection column
      #
      def collection_column?
        false
      end

      #
      # @param type_in [Symbol,Type] type to check against
      # @return [Boolean] true if this column has the type given by `type_in`
      #
      def type?(type_in)
        type == Type[type_in]
      end

      #
      # @param value the value to cast
      # @return the value cast to the appropriate type for this column
      #
      # @api private
      #
      def cast(value)
        @type.cast(value)
      end

      #
      # @return [String] a CQL fragment representing this column in a table
      #   definition
      #
      # @api private
      #
      def to_cql
        "#{@name} #{@type}"
      end

      #
      # @param other [Column] a column object
      # @return [Boolean] true if this column has the same CQL representation
      #   as `other` column
      #
      def ==(other)
        to_cql == other.to_cql
      end

      #
      # @return [String] the column's name
      #
      def to_s
        name.to_s
      end

      #
      # @return [String] human-readable representation of this column
      #
      def inspect
        %Q(#<#{self.class.name}: #{to_cql}>)
      end
    end

    #
    # A partition key column
    #
    class PartitionKey < Column
      #
      # (see Column#partition_key?)
      #
      def partition_key?
        true
      end
    end

    #
    # A clustering column
    #
    class ClusteringColumn < Column
      #
      # @return [:asc,:desc] whether rows are ordered by ascending or
      #   descending values in this column
      #
      attr_reader :clustering_order

      #
      # @param (see Column#initialize)
      # @param clustering_order [:asc,:desc] ascending or descending order for
      #   this column
      #
      def initialize(name, type, clustering_order = nil)
        super(name, type)
        @clustering_order = (clustering_order || :asc).to_sym
      end

      #
      # (see Column#clustering_column?)
      #
      def clustering_column?
        true
      end

      # @private
      def clustering_order_cql
        "#{@name} #{@clustering_order}"
      end
    end

    #
    # A scalar data column
    #
    class DataColumn < Column
      #
      # @return [Symbol] name of the secondary index applied to this column, if
      #   any
      #
      attr_reader :index_name

      #
      # @param (see Column#initialize)
      # @param index_name [Symbol] name this column's secondary index
      #
      def initialize(name, type, index_name = nil)
        super(name, type)
        @index_name = index_name
      end

      #
      # @return [Boolean] true if this column has a secondary index
      #
      def indexed?
        !!@index_name
      end
    end

    #
    # A collection column (list, set, or map)
    #
    # @abstract
    #
    class CollectionColumn < Column
      # (see Column#collection_column?)
      def collection_column?
        true
      end

      # (see DataColumn#indexed?)
      def indexed?
        false
      end
    end

    #
    # A List column
    #
    # @see http://cassandra.apache.org/doc/cql3/CQL.html#list
    #   CQL documentation for the list type
    #
    class List < CollectionColumn
      # (see Column#to_cql)
      def to_cql
        "#{@name} LIST <#{@type}>"
      end

      #
      # @return [Array] array with elements cast to correct type for column
      #
      def cast(value)
        value.map { |element| @type.cast(element) }
      end
    end

    #
    # A Set column
    #
    # @see http://cassandra.apache.org/doc/cql3/CQL.html#set
    #   CQL documentation for set columns
    #
    class Set < CollectionColumn
      # (see Column#to_cql)
      def to_cql
        "#{@name} SET <#{@type}>"
      end

      #
      # @param (see Column#cast)
      # @return [::Set] set with elements cast to correct type for column
      #
      def cast(value)
        value.to_set { |element| @type.cast(element) }
      end
    end

    #
    # A Map column
    #
    # @see
    #   http://cassandra.apache.org/doc/cql3/CQL.html#map
    #   CQL documentation for map columns
    #
    class Map < CollectionColumn
      # @return [Type] the type of keys in this map
      attr_reader :key_type
      alias_method :value_type, :type

      #
      # @param name [Symbol] name of this column
      # @param key_type [Type] type of the keys in the map
      # @param value_type [Type] type of the values in the map
      #
      def initialize(name, key_type, value_type)
        super(name, value_type)
        @key_type = key_type
      end

      # (see Column#to_cql)
      def to_cql
        "#{@name} MAP <#{@key_type}, #{@type}>"
      end

      #
      # @param (see Column#cast)
      # @return [Hash] hash with keys and values cast to correct type for
      #   column
      #
      def cast(value)
        value.each_with_object({}) do |(key, element), hash|
          hash[@key_type.cast(key)] = @type.cast(element)
        end
      end
    end
  end
end
