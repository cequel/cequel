module Cequel

  class Keyspace

    #
    # @api private
    # @see Cequel.connect
    #
    def initialize(connection)
      @connection = connection
    end

    #
    # Get a handle to a ColumnGroup in this keyspace.
    #
    # @param column_group_name [Symbol] the name of the column group
    # @return [ColumnGroup] a column group
    #
    def [](column_group_name)
      ColumnGroup.new(column_group_name.to_sym, @connection)
    end

  end

end
