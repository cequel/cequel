module Cequel

  module Model

    module Scoped

      extend ActiveSupport::Concern

      included do
        extend MonitorMixin
      end

      module ClassMethods
        delegate :consistency, :count, :first, :limit, :select, :where,
          :to => :all

        def all
          @_cequel.current_scope || Scope.new(self, column_family)
        end

        def select(*rows)
          all.select(*rows)
        end

        def with_scope(scope)
          synchronize do
            old_scope = @_cequel.current_scope
            begin
              @_cequel.current_scope = scope
              yield
            ensure
              @_cequel.current_scope = old_scope
            end
          end
        end

      end

    end

  end

end
