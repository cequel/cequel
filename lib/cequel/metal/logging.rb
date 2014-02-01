module Cequel
  module Metal
    #
    # Methods to handle logging for {Keyspace} instances
    #
    module Logging
      def logger=(logger)
        loggers << Logger.new(logger, ::Logger::DEBUG)
        self.exception_logger = ExceptionLogger.new(logger, ::Logger::ERROR)
      end

      def slowlog=(slowlog)
        warn "#slowlog= is deprecated and will be removed from a future " \
             "version"
        loggers << @slowlog = Logger.new(slowlog, ::Logger::WARN, 2000)
      end

      def slowlog_threshold=(threshold)
        @slowlog.threshold = threshold
      end

      protected

      attr_accessor :exception_logger

      private

      def log(label, statement, *bind_vars)
        response = nil
        begin
          time = Benchmark.ms { response = yield }
          loggers.each do |logger|
            logger.log(label, time, statement, bind_vars)
          end
        rescue Exception => e
          exception_logger.log(label, statement, bind_vars) if exception_logger
          raise
        end
        response
      end

      def loggers
        @loggers ||= []
      end
    end
  end
end
