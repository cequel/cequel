module Cequel

  module Record

    module SecondaryIndexes

      def column(name, type, options = {})
        super
        name = name.to_sym
        if options[:index]
          instance_eval <<-RUBY, __FILE__, __LINE__+1
            def with_#{name}(value)
              all.where(#{name.inspect}, value)
            end

            def find_by_#{name}(value)
              with_#{name}(value).first
            end

            def find_all_by_#{name}(value)
              with_#{name}(value).to_a
            end
          RUBY
        end
      end

    end

  end

end
