module Cequel

  module Model

    module Persistence

      extend ActiveSupport::Concern

      module ClassMethods

        def find(*keys)
          coerce_array = keys.first.is_a?(Array)
          keys.flatten!
          if keys.length == 1
            instance = find_one(keys.first)
            coerce_array ? [instance] : instance
          else
            find_many(keys)
          end
        end

        def column_family_name
          name.tableize.to_sym
        end

        def column_family
          keyspace[column_family_name]
        end

        def keyspace
          Cequel::Model.keyspace
        end

        private
        
        def find_one(key)
          row = column_family.where(key_alias => key).first
          _hydrate(row).tap do |result|
            raise RecordNotFound,
              "Couldn't find #{name} with #{key_alias}=#{key}" if result.nil?
          end
        end

        def find_many(keys)
          results = column_family.where(key_alias => keys).
            map { |row| _hydrate(row) }.compact

          if results.length < keys.length
            raise RecordNotFound,
              "Couldn't find all #{name.pluralize} with #{key_alias} (#{keys.join(', ')})" <<
                "(found #{results.length} results, but was looking for #{keys.length}"
          end
          results
        end

        def _hydrate(row)
          unless row.length == 1
            new(row[key_alias])._hydrate(row.except(key_alias))
          end
        end

      end

      def _hydrate(row)
        tap { @_cequel.attributes = row }
      end

    end

  end

end
