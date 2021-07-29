# -*- encoding : utf-8 -*-
module Cequel
  #
  # This module adds some utility methods for generating and type-checking UUID
  # objects for use with Cequel. These methods are provided because the actual
  # UUID implementation is an artifact of the underlying driver;
  # initializing/typechecking those driver classes directly is potentially
  # breaking.
  #
  module Uuids
    #
    # Create a UUID
    #
    # @param value [Time,String,Integer] timestamp to assign to the UUID, or
    #   numeric or string representation of the UUID
    # @return a UUID appropriate for use with Cequel
    #
    def uuid(value = nil)
      if value.nil?
        timeuuid_generator.now
      elsif value.is_a?(Time)
        timeuuid_generator.at(value)
      elsif value.is_a?(DateTime)
        timeuuid_generator.at(Time.at(value.to_f))
      else
        Type::Timeuuid.instance.cast(value)
      end
    end

    #
    # Determine if an object is a UUID
    #
    # @param object an object to check
    # @return [Boolean] true if the object is recognized by Cequel as a UUID
    #
    def uuid?(object)
      return true if uuid_in_string?(object)

      object.is_a?(Cassandra::Uuid)
    end

    private

    def uuid_in_string?(object)
      object.is_a?(String) && Cassandra::Uuid.new(object)
    rescue ArgumentError
      false
    end

    def timeuuid_generator
      current_pid = Process.pid
      if Thread.current[:cequel_timeuuid_generator_pid] != current_pid
        Thread.current[:cequel_timeuuid_generator_pid] = current_pid
        # Clearing the thread local generator ensures that a forked child process will not use a
        # generator with the same internal state as one held by the parent process.
        Thread.current[:cequel_timeuuid_generator] = nil
      end
      Thread.current[:cequel_timeuuid_generator] ||= Cassandra::TimeUuid::Generator.new
    end
  end
end
