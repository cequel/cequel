# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    module Policy
      module CassandraError
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