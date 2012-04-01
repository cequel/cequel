module Cequel

  class Keyspace

    def initialize(connection)
      @connection = connection
    end

    def [](column_group)
      ColumnGroup.new(column_group.to_sym, @connection)
    end

  end

end
