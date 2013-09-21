module Cequel

  module Util

    module HashAccessors

      def hattr_reader(hash, *attributes)
        attributes.each do |attribute|
          module_eval <<-RUBY, __FILE__, __LINE__+1
            def #{attribute}
              #{hash}[#{attribute.to_sym.inspect}]
            end
          RUBY
        end
      end

      def hattr_inquirer(hash, *attributes)
        attributes.each do |attribute|
          module_eval <<-RUBY, __FILE__, __LINE__+1
            def #{attribute}?
              !!#{hash}[#{attribute.to_sym.inspect}]
            end
          RUBY
        end
      end

      def hattr_writer(hash, *attributes)
        attributes.each do |attribute|
          module_eval <<-RUBY, __FILE__, __LINE__+1
            def #{attribute}=(value)
              #{hash}[#{attribute.to_sym.inspect}] = value
            end
          RUBY
        end
      end

      def hattr_accessor(hash, *attributes)
        hattr_reader(hash, *attributes)
        hattr_writer(hash, *attributes)
      end

    end

  end
  
end
