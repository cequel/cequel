module Cequel

  module Model

    module Scoped

      extend Forwardable

      def_delegators :current_scope, *RecordSet.instance_methods(false)

      def current_scope
        RecordSet.create(self)
      end

    end

  end

end
