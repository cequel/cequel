# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # Methods to handle logging for {Keyspace} instances
    #
    module Logging
      extend Util::Forwardable
      def_delegators :request_logger, :logger, :logger=, :slowlog_threshold,
                     :slowlog_threshold=

      #
      # @deprecated
      #
      def slowlog=(slowlog)
        warn "#slowlog= is deprecated and ignored"
      end

      protected

      attr_writer :request_logger

      def request_logger
        @request_logger ||= RequestLogger.new
      end

      def_delegator :request_logger, :log
      protected :log
    end
  end
end
