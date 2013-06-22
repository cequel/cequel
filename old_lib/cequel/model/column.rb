module Cequel

  module Model

    #
    # Encapsulates information about a column in a model's column family
    #
    class Column
      attr_reader :name, :type, :default

      def initialize(name, type, options = {})
        @name, @type = name, type
        @default = options[:default]
      end

    end

  end

end
