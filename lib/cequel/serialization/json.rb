module Cequel

  module Serialization

    class Json < Base

      def to_s
        Oj.dump(obj)
      end

    end

  end

end
