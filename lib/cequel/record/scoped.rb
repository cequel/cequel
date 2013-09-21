module Cequel

  module Record

    module Scoped

      extend Forwardable

      def_delegators :current_scope,
        *(RecordSet.public_instance_methods(false) - Object.instance_methods)

      def current_scope
        Thread.current["#{name}::current_scope"] || RecordSet.new(self)
      end

      def with_scope(record_set)
        previous_scope = current_scope
        self.current_scope = record_set
        yield
      ensure
        self.current_scope = previous_scope
      end

      protected

      def current_scope=(current_scope)
        Thread.current["#{name}::current_scope"] = current_scope
      end

    end

  end

end
