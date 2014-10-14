module Cequel

  module Model

    class ReadableDictionary

      # Cassandra will only fetch the first 10000 column, when dictionaries have
      # more columsn that that, we have to handle the case
      CASSANDRA_COLUMN_LIMIT = 10000

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
              map do |row|
            dict = new(row.delete(key_alias.to_s), row)
            if row.count >= CASSANDRA_COLUMN_LIMIT
              dict.load_remaining
            end
            dict
          end
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
        @loaded ?  @row.first : slice(:first => 1).first
      end

      def last
        if @loaded
          unless @row.empty?
            key = @row.keys.last
            [key, @row[key]]
          end
        else
          slice(:last => 1).first
        end
      end

      def key?(column)
        @row.key?(column) || load_raw_slice([column])[column].present?
      end

      def each_pair(options = {}, &block)
        return to_enum(:each_pair, options) unless block
        return @row.each_pair(&block) if @loaded && !options[:force_load]
        batch_size = options[:batch_size] || self.class.default_batch_size
        each_slice(batch_size, options[:from]) do |batch_results|
          batch_results.each_pair(&block)
        end
      end

      def each(&block)
        each_pair(&block)
      end

      def each_slice(batch_size, last_key=nil)
        batch_scope = scope.select(:first => batch_size)
        batch_scope = batch_scope.select(:from => last_key) if last_key
        key_alias = self.class.key_alias
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
        return self if @loaded
        @row ||= {}
        each_pair { |column, value| @row[column] = value }
        @loaded = true
        self
      end

      def loaded?
        !!@loaded
      end

      def load_remaining
        each_pair(force_load: true, from: @row.keys.last, batch_size: CASSANDRA_COLUMN_LIMIT) { |column, value| @row[column] = value }
        @loaded = true
        self
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
