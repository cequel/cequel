# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    module Policy
      module CassandraError
        # This mixin is used by the Keyspace object to dictate 
        # how a failure from Cassandra is handled.
        # The only method defined is 
        #   handle_error(error, retries_remaining)
        # The first argument is the error from the Cassandra gem.
        # The second argument is the number of remaining retries.
        # This function must raise the error to abort the operation,
        # if this function returns it indicates the operation should be 
        # retried.
        # 
        # The specific mixin is chosen by passing configuration options
        # See Keyspace#configure 
        module ClearAndRetry
          def handle_error(error, retries_remaining)
            clear_active_connections!
            raise error if retries_remaining == 0
            sleep(retry_delay)          
          end
        end
        
        module Retry
          def handle_error(error, retries_remaining)
            raise error if retries_remaining == 0
            sleep(retry_delay)          
          end
        end
        
        module Raise
          def handle_error(error, retries_remaining)
            raise error
          end
        end
      end 
    end
  end 
end