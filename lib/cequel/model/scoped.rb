module Cequel

  module Model

    module Scoped

      extend Forwardable

      def_delegators :current_scope, *Scope.instance_methods(false)

      def current_scope
        Scope.new(self)
      end

    end

  end

end
