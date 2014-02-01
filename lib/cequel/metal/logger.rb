module Cequel
  module Metal
    class Logger
      attr_reader :out, :severity
      attr_accessor :threshold

      def initialize(out, severity, threshold = 0)
        @out, @severity, @threshold = out, severity, threshold
      end

      def log(label, timing, statement, bind_vars)
        if timing >= threshold
          out.add(severity) do
            sprintf(
              '%s (%dms) %s',
              label, timing, sanitize(statement, bind_vars)
            )
          end
        end
      end

      private

      delegate :sanitize, :to => 'CassandraCQL::Statement'
    end

    class ExceptionLogger < Logger
      def log(label, statement, bind_vars)
        out.add(severity) do
          sprintf('%s (ERROR) %s', label, sanitize(statement, bind_vars))
        end
      end
    end
  end
end
