module Cequel

  module Model

    class Scope

      extend Forwardable
      include Enumerable

      def initialize(clazz)
        @clazz = clazz
      end

      def all
        self
      end

      def select(*columns)
        return super if block_given?
        scoped { |data_set| data_set.select(*columns) }
      end

      def limit(count)
        scoped { |data_set| data_set.limit(count) }
      end

      def first(count = nil)
        count ? limit(count).entries : limit(1).each.first
      end

      def count
        data_set.count
      end

      def each(&block)
        find_each(&block)
      end

      def find_each(options = {})
        return enum_for(:find_each, options) unless block_given?
        find_each_row(options) { |row| yield clazz.hydrate(row) }
      end

      def find_each_row(options = {}, &block)
        return enum_for(:find_each_row, options) unless block
        find_rows_in_batches(options) { |row| row.each(&block) }
      end

      def find_rows_in_batches(options = {})
        if row_limit
          if options.key?(:batch_size)
            raise ArgumentError,
              "Can't pass :batch_size argument with a limit in the scope"
          else
            yield data_set.entries
            return
          end
        end
        batch_size = options.fetch(:batch_size, 1000)
        base_batch_data_set = data_set.limit(options.fetch(:batch_size, 1000))
        batch_data_set = base_batch_data_set
        key_column = clazz.local_key_column.name
        begin
          batch = batch_data_set.entries
          yield batch
          if batch.any?
            batch_data_set = base_batch_data_set.
              where("TOKEN(#{key_column}) > TOKEN(?)", batch.last[key_column])
          end
        end while batch.length == batch_size
      end

      protected
      attr_writer :data_set

      def data_set
        @data_set ||= connection[clazz.table_name]
      end

      private
      attr_reader :clazz
      def_delegators :clazz, :connection
      def_delegators :data_set, :row_limit

      def scoped
        self.class.new(clazz).
          tap { |scope| scope.data_set = yield(data_set) }
      end

    end

  end

end
