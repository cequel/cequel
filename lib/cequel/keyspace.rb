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
      DataSet.new(column_group_name.to_sym, @connection)
    end

  end

end
