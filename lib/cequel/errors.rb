# -*- encoding : utf-8 -*-
module Cequel
  #
  # @since 1.0.0
  #
  # Raised when the schema defined in Cassandra cannot be modified to match
  # the schema defined in the application (e.g., changing the type of a primary
  # key)
  #
  InvalidSchemaMigration = Class.new(StandardError)

  NoSuchKeyspaceError = Class.new(StandardError)

  NoSuchTableError = Class.new(StandardError)
end
