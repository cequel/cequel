module Cequel

  module Model

    #
    # Encapsulates information about a column in a model's column family
    #
    class Column
      attr_reader :name, :type

      def initialize(name, type)
        @name, @type = name, type
      end
    end

  end

end
