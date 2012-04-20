module Cequel

  module Helpers

    private

    def sanitize(statement, *bind_vars)
      bind_vars.map! { |var| SimpleUUID::UUID === var ? var.to_guid : var }
      CassandraCQL::Statement.sanitize(statement, bind_vars)
    end

  end

end
