module Cequel

  module Model

    module Scoped

      extend ActiveSupport::Concern

      module ClassMethods
        delegate :consistency, :count, :first, :limit, :select, :where,
          :find_in_batches, :find_each, :find_rows_in_batches, :find_each_row,
          :to => :all

        def default_scope(scope)
          @_cequel.default_scope = scope
        end

        def all
          current_scope || @_cequel.default_scope || empty_scope
        end

        def select(*rows)
          all.select(*rows)
        end

        def with_scope(scope)
          @_cequel.synchronize do
            old_scope = current_scope
            begin
              self.current_scope = scope
              yield
            ensure
              self.current_scope = old_scope
            end
          end
        end

        private

        def empty_scope
          Scope.new(self, [column_family])
        end

        def current_scope
          ::Thread.current[current_scope_key]
        end

        def current_scope=(scope)
          ::Thread.current[current_scope_key] = scope
        end

        def current_scope_key
          :"cequel-current_scope-#{object_id}"
        end

      end

    end

  end

end
