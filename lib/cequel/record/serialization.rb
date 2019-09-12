# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # `Cequel::Record` implementations use a number of different types
    # of columns than ActiveRecord columns. In later versions of Ruby (2.6.3+)
    # some of tehse column types (such as a Cequel::TimeUUID) are not
    # serialized correctly.  Rails provides a method to handle this wtih a
    # hook built in to take control of what is represented as a 
    # serializable hash. This module overrides that funcionality to allow
    # Cequel ot handle these new cases and provide correct serializable data.
    #
    # @since 3.2.2
    #
    module Serialization
      # Hook method defining how an attribute value should be retrieved for
      # serialization. By default this is assumed to be an instance named after
      # the attribute. Override this method in subclasses should you need to
      # retrieve the value for a given attribute differently:
      #
      #   class MyClass
      #     include ActiveModel::Serialization
      #
      #     def initialize(data = {})
      #       @data = data
      #     end
      #
      #     def read_attribute_for_serialization(key)
      #       @data[key]
      #     end
      #   end
      def read_attribute_for_serialization(key)
        value = attributes[key]
        value = cast_time_uuid_to_hash(value)
        value
      end

      # Converts the value of a TimeUuid field to the hash representation
      # of this value
      def cast_time_uuid_to_hash(value)
        return value unless value.is_a?(Cassandra::TimeUuid)

        {"n" => value.to_i, "s" => value.to_s}
      end
    end
  end
end