module Cequel
  module Record
    module Finders
      private

      def key(*)
        if key_columns.any?
          def_finder('find_all_by', '.entries')
          undef_finder('find_by')
        end
        super
        def_finder('find_by', '.first')
        def_finder('with')
      end

      def def_finder(finder_method_prefix, scope_operation = '')
        arg_names = key_column_names.join(', ')
        column_filter_expr = key_column_names
          .map { |name| "#{name}: #{name}" }.join(', ')

        singleton_class.module_eval(<<-RUBY, __FILE__, __LINE__+1)
          def #{finder_method_prefix}_#{finder_method_suffix}(#{arg_names})
            where(#{column_filter_expr})#{scope_operation}
          end
        RUBY
      end

      def undef_finder(finder_method_prefix)
        method_name = "#{finder_method_prefix}_#{finder_method_suffix}"
        singleton_class.module_eval { undef_method(method_name) }
      end

      def finder_method_suffix
        key_column_names.join('_and_')
      end
    end
  end
end
