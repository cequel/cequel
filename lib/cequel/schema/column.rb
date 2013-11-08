module Cequel

  module Schema

    class Column

      attr_reader :name, :type

      def initialize(name, type)
        @name, @type = name, type
      end

      def key?
        partition_key? || clustering_column?
      end

      def partition_key?
        false
      end

      def type?(type_in)
        type.is_a?(type_in)
      end

      def clustering_column?
        false
      end

      def data_column?
        !key?
      end

      def to_cql
        "#{@name} #{@type}"
      end

      def cast(value)
        @type.cast(value)
      end

      def ==(other)
        to_cql == other.to_cql
      end

      def to_s
        name
      end

      def inspect
        %Q(#<#{self.class.name}: #{to_cql}>)
      end

    end

    class PartitionKey < Column

      def partition_key?
        true
      end

    end

    class ClusteringColumn < Column

      attr_reader :clustering_order

      def initialize(name, type, clustering_order = nil)
        super(name, type)
        @clustering_order = (clustering_order || :asc).to_sym
      end

      def clustering_order_cql
        "#{@name} #{@clustering_order}"
      end

      def clustering_column?
        true
      end

    end

    class DataColumn < Column

      attr_reader :index_name

      def initialize(name, type, index_name = nil)
        super(name, type)
        @index_name = index_name
      end

      def indexed?
        !!@index_name
      end

    end

    class CollectionColumn < Column

      def indexed?
        false
      end

    end

    class List < CollectionColumn

      def to_cql
        "#{@name} LIST <#{@type}>"
      end

      def cast(value)
        value.map { |element| @type.cast(element) }
      end

    end

    class Set < CollectionColumn

      def to_cql
        "#{@name} SET <#{@type}>"
      end

      def cast(value)
        value.each_with_object(::Set[]) do |element, set|
          set << @type.cast(element)
        end
      end

    end

    class Map < CollectionColumn

      attr_reader :key_type
      alias_method :value_type, :type

      def initialize(name, key_type, value_type)
        super(name, value_type)
        @key_type = key_type
      end

      def to_cql
        "#{@name} MAP <#{@key_type}, #{@type}>"
      end

      def cast(value)
        value.each_with_object({}) do |(key, element), hash|
          hash[@key_type.cast(key)] = @type.cast(element)
        end
      end

    end

  end

end
