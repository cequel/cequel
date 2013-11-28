module Cequel
  module Record
    module BulkWrites
      def update_all(attributes)
        key_attributes_for_each_row.each_slice(100) do |batch|
          connection.batch(:unlogged => true) do
            batch.each do |key_attributes|
              keyspace.where(key_attributes).update(attributes)
            end
          end
        end
      end
    end
  end
end
