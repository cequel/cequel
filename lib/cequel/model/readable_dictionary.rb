module Cequel

  module Model

    class ReadableDictionary

      class <<self

        attr_writer :column_family, :default_batch_size

        def key_alias
          @key_alias ||= :KEY
        end

        def key_type
          @key_type ||= :text
        end

        def comparator
          @comparator ||= :text
        end

        def validation
          :counter
        end

        def key(key_alias, type)
          @key_alias, @key_type = key_alias, type

          module_eval(<<-RUBY)
          def #{key_alias.downcase}
            @key
          end
          RUBY
        end

        def columns(comparator)
          @comparator = comparator
        end

        def column_family
          return @column_family if @column_family
          self.column_family_name = name.underscore.to_sym
          @column_family
        end

        def column_family_name=(column_family_name)
          self.column_family = Cequel::Model.keyspace[column_family_name]
        end

        def default_batch_size
          @default_batch_size || 1000
        end

        def [](key)
          new(key)
        end
        private :new

        def load(*keys)
          keys.flatten!
          column_family.
            where(key_alias.to_s => keys).
            map { |row| new(row.delete(key_alias.to_s), row) }
        end

      end

      include Enumerable

      def initialize(key, row = nil)
        @key = key
        setup(row)
      end

      def [](column)
        if @loaded
          @row[column]
        else
          scope.select(column).first[column]
        end
      end

      def keys
        @loaded ? @row.keys : each_pair.map { |key, value| key }
      end

      def values
        @loaded ? @row.values : each_pair.map { |key, value| value }
      end

      def slice(*columns)
        if @loaded
          @row.slice(*columns)
        else
          deserialize_row(load_raw_slice(columns))
        end
      end

      def first
        slice(:first => 1).first
      end

      def last
        slice(:last => 1).first
      end

      def key?(column)
        @row.key?(column) || load_raw_slice([column])[column].present?
      end

      def each_pair(options = {}, &block)
        return Enumerator.new(self, :each_pair, options) unless block
        return @row.each_pair(&block) if @loaded
        batch_size = options[:batch_size] || self.class.default_batch_size
        each_slice(batch_size) do |batch_results|
          batch_results.each_pair(&block)
        end
      end

      def each(&block)
        each_pair(&block)
      end

      def each_slice(batch_size)
        batch_scope = scope.select(:first => batch_size)
        key_alias = self.class.key_alias
        last_key = nil
        begin
          batch_results = batch_scope.first
          batch_results.delete(key_alias)
          result_length = batch_results.length
          batch_results.delete(last_key) unless last_key.nil?
          yield deserialize_row(batch_results)
          last_key = batch_results.keys.last
          batch_scope = batch_scope.select(:from => last_key)
        end while result_length == batch_size
      end

      def load
        @row = {}
        each_pair { |column, value| @row[column] = value }
        @loaded = true
        self
      end

      def loaded?
        !!@loaded
      end

      private

      def setup(init_row = nil)
        @row = deserialize_row(init_row || {})
        @loaded = !!init_row
      end

      def scope
        self.class.column_family.where(self.class.key_alias => @key)
      end

      def load_raw_slice(columns)
        row = scope.select(*columns).first.except(self.class.key_alias)
      end

      def deserialize_row(row)
        row
      end

    end

  end

end
