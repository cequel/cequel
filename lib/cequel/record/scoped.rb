module Cequel

  module Record

    module Scoped

      extend Forwardable

      def_delegators :current_scope,
        *(RecordSet.public_instance_methods(false) - Object.instance_methods)

      def current_scope
        RecordSet.create(self)
      end

    end

  end

end
