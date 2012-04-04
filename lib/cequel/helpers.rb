module Cequel

  module Helpers

    private

    def sanitize(statement, *bind_vars)
      CassandraCQL::Statement.sanitize(statement, bind_vars)
    end

  end

end
