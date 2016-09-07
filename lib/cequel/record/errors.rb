# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # Raised when attempting to access an attribute of a record when that
    # attribute hasn't been loaded
    #
    # @since 1.0.0
    #
    MissingAttributeError = Class.new(ArgumentError)

    #
    # Raised when attempting to read or write an attribute that isn't defined
    # on the record
    #
    # @since 1.0.0
    #
    UnknownAttributeError = Class.new(ArgumentError)

    #
    # Raised when attempting to load a record by key when that record does not
    # exist
    #
    RecordNotFound = Class.new(StandardError)

    #
    # Raised when attempting to configure a record in a way that is not
    # possible
    #
    # @since 1.0.0
    #
    InvalidRecordConfiguration = Class.new(StandardError)

    #
    # Raised when attempting to save a record that is invalid
    #
    RecordInvalid = Class.new(StandardError)

    #
    # Raised when attempting to construct a {RecordSet} that cannot construct
    # a valid CQL query
    #
    # @since 1.0.0
    #
    IllegalQuery = Class.new(StandardError)

    #
    # Raised when attempting to perform a query that has detrimental effects.
    # Typically when trying to count records.
    #
    DangerousQueryError = Class.new(StandardError)

    #
    # Raised when attempting to persist a Cequel::Record without defining all
    # primary key columns
    #
    # @since 1.0.0
    #
    MissingKeyError = Class.new(StandardError)

    #
    # Raised when attempting to reflect on the schema of a
    # Cequel::Record without a table name.
    #
    MissingTableNameError = Class.new(StandardError)
  end
end
