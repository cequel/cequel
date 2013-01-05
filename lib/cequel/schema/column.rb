module Cequel

  module Schema

    class Column

      attr_reader :name, :type

      def initialize(name, type)
        @name, @type = name, type
      end

      def to_cql
        "#{@name} #{@type}"
      end

    end

    class PartitionKey < Column; end

    class NonpartitionKey < Column

      attr_reader :clustering_order

      def initialize(name, type, clustering_order = nil)
        super(name, type)
        @clustering_order = (clustering_order || :asc).to_sym
      end

      def clustering_order_cql
        "#{@name} #{@clustering_order}"
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

    end

    class Set < CollectionColumn

      def to_cql
        "#{@name} SET <#{@type}>"
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

    end

  end

end
