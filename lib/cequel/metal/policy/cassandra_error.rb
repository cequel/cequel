# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    module Policy
      module CassandraError
        class ErrorPolicyBase                  
          # This class is used by the Keyspace object to dictate 
          # how a failure from Cassandra is handled.
          # The only method defined is 
          #   handle_error(keyspace, error, retries_remaining)
          # The first argument is an instance of the Keyspace class
          # The second argument is the error from the Cassandra gem.
          # The thid argument is the number of remaining retries.
          # This function must raise the error to abort the operation,
          # if this function returns it indicates the operation should be 
          # retried.
          # 
          # The specific instance is chosen by passing configuration options
          # See Keyspace#configure
          
          # On instantiation, the configuraiton hash passed to Cequel is 
          # available here
          def initialize(options = {})
          end
                    
          def handle_error(error, retries_remaining)
            raise RuntimeError, 'This is an abstract base class, never call this'
          end
        end
         
        class ClearAndRetryPolicy < ErrorPolicyBase 
          def handle_error(keyspace, error, retries_remaining)
            keyspace.clear_active_connections!
            raise error if retries_remaining == 0
            sleep(keyspace.retry_delay)          
          end
        end
        
        class RetryPolicy < ErrorPolicyBase
          def handle_error(keyspace, error, retries_remaining)
            raise error if retries_remaining == 0
            sleep(keyspace.retry_delay)          
          end
        end
        
        class RaisePolicy < ErrorPolicyBase
          def handle_error(keyspace, error, retries_remaining)
            raise error
          end
        end
      end 
    end
  end 
end