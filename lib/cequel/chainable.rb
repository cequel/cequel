module Cequel
  # Provides declarative way of making methods return the receiver.
  #
  # Examples
  #
  #     class Cequel::Foo
  #       extend Chainable
  #
  #       def bar
  #         # do useful work
  #       end
  #       chainable :bar
  #     end
  #
  #     Cequel::Foo.new.bar #=> #<Cequel::Foo:0x007fef4b864808>
  module Chainable
    protected

    # Ensure that the specified method returns the receiver object.
    #
    # @param method_name [String] name of method to make return self.
    def chainable(method_name)
      if respond_to? :prepend
        prepend build_chainable_module_for(method_name)
      else
        define_method method_name, &build_chainable_method_for(method_name)
      end
    end

    # Implementation for legacy rubies.
    def build_chainable_method_for(method_name)
      orig_method = self.instance_method(method_name)
      ->(*args) {
        orig_method.bind(self).call(*args)
        self
      }
    end

    # Implementation for modern rubies.
    def build_chainable_module_for(method_name)
      Module.new do
        define_method(method_name) do |*args|
          super(*args)
          self
        end
      end
    end
  end
end