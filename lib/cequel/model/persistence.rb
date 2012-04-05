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

        def _hydrate(row)
          unless row.length == 1
            new(row[key_alias])._hydrate(row.except(key_alias))
          end
        end

        private
        
        def find_one(key)
          all.where(key_alias => key).first.tap do |result|
            raise RecordNotFound,
              "Couldn't find #{name} with #{key_alias}=#{key}" if result.nil?
          end
        end

        def find_many(keys)
          results = all.where(key_alias => keys).to_a
          results.compact!

          if results.length < keys.length
            raise RecordNotFound,
              "Couldn't find all #{name.pluralize} with #{key_alias} (#{keys.join(', ')})" <<
                "(found #{results.length} results, but was looking for #{keys.length}"
          end
          results
        end

      end

      def save
        persisted? ? update : insert
      end

      def insert
        return if @_cequel.attributes.empty?
        self.class.column_family.insert(attributes)
        persisted!
      end

      def update
        update_attributes, delete_attributes = {}, []
        changes.each_pair do |attr, (old, new)|
          if new.nil?
            delete_attributes << attr
          else
            update_attributes[attr] = new
          end
        end
        data_set.update(update_attributes) if update_attributes.any?
        data_set.delete(*delete_attributes) if delete_attributes.any?
        transient! if @_cequel.attributes.empty?
      end

      def destroy
        data_set.delete
      end

      def _hydrate(row)
        tap do
          @_cequel.attributes = row
          persisted!
        end
      end

      def persisted!
        @_cequel.persisted = true
      end

      def transient!
        @_cequel.persisted = false
      end

      def persisted?
        !!@_cequel.persisted
      end

      private

      def data_set
        self.class.column_family.
          where(self.class.key_alias => @_cequel.key)
      end

    end

  end

end
