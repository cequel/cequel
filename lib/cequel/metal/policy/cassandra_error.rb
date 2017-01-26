# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    module Policy
      module CassandraError
        class ErrorPolicyBase                  
          # This class is used by the Keyspace object to dictate 
          # how a failure from Cassandra is handled.
          # The only method defined is 
          #   execute_stmt(keyspace)
          # The first argument is an instance of the Keyspace class
          # This method may raise an error to abort the operation,
          # 
          # The specific instance is chosen by passing configuration options
          # See Keyspace#configure
          
          # On instantiation, the configuraiton hash passed to Cequel is 
          # available here
          def initialize(options = {})
          end
                    
          def execute_stmt(keyspace)
            raise NotImplementedError, "#execute_stmt must be implemented in #{self.class.name}"
          end
        end
         
        class ClearAndRetryPolicy < ErrorPolicyBase 
          # @return Integer maximum number of retries to reconnect to Cassandra
          attr_reader :max_retries
          # @return Float delay between retries to reconnect to Cassandra
          attr_reader :retry_delay
          # @return Boolean if this policy clears connections before retry
          attr_reader :clear_before_retry
          def initialize(options = {})
            @max_retries = options.fetch(:max_retries, 3)
            @retry_delay = options.fetch(:retry_delay, 0.5)
            @clear_before_retry = !!options.fetch(:clear_before_retry, true)
            
            if @retry_delay <= 0.0
              raise ArgumentError, "The value for retry must be a positive number, not '#{@retry_delay}'"
            end
          end 
          
          def execute_stmt(keyspace)
            retries_remaining = max_retries
            begin
              yield
            rescue Cassandra::Errors::NoHostsAvailable,
                  Cassandra::Errors::ExecutionError,
                  Cassandra::Errors::TimeoutError => error
              raise error if retries_remaining == 0
              sleep(retry_delay)
              keyspace.clear_active_connections! if clear_before_retry
              retries_remaining -= 1
              retry                    
            end
          end 
        end
        
        module RetryPolicy 
          def self.new(options = {})
            options[:clear_before_retry] = false 
            ClearAndRetryPolicy.new(options)
          end 
        end
        
        class RaisePolicy < ErrorPolicyBase
          def execute_stmt(keyspace)
            yield
          end           
        end
      end 
    end
  end 
end 
