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
    # Get DataSet encapsulating a column group in this keyspace
    #
    # @param column_group_name [Symbol] the name of the column group
    # @return [DataSet] a column group
    #
    def [](column_group_name)
      DataSet.new(column_group_name.to_sym, self)
    end

    #
    # Execute a CQL statement on this keyspace
    #
    def execute(statement, *bind_vars)
      @connection.execute(statement, *bind_vars)
    end

  end

end
