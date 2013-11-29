module Cequel
  #
  # @abstract
  # @since 1.0.0
  # Base class for all errors raised by Cequel
  #
  Error = Class.new(StandardError)
  #
  # @since 1.0.0
  #
  # Raised when the schema defined in Cassandra cannot be modified to match
  # the schema defined in the application (e.g., changing the type of a primary
  # key)
  #
  InvalidSchemaMigration = Class.new(Error)
  #
  # @since 1.0.0
  #
  # Raised when attempting to persist a Cequel::Record without defining all
  # primary key columns
  #
  MissingKeyError = Class.new(Error)
end
