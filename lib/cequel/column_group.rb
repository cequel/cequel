module Cequel

  class ColumnGroup

    def initialize(name, connection)
      @name, @connection = name, connection
    end

    def insert(data)
      cql = "INSERT INTO #{@name} " <<
        "(#{data.keys.join(', ')}) " <<
        "VALUES (" << (['?'] * data.length).join(', ') << ")"
      @connection.execute(cql, *data.values)
    end

  end

end
