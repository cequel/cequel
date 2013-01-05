module Cequel

  module Schema

    class TableProperty

      attr_reader :name, :value

      def initialize(name, value)
        @name = name
        set_normalized_value(value)
      end

      def to_cql
        if Hash === @value
          map_pairs = @value.each_pair.
            map { |key, value| "#{quote(key.to_s)} : #{quote(value)}" }.
            join(', ')
          value_cql = "{ #{map_pairs} }"
        else
          value_cql = quote(@value)
        end
        "#{@name} = #{value_cql}"
      end

      private

      def quote(value)
        CassandraCQL::Statement.quote(value)
      end

      def set_normalized_value(map)
        return @value = map unless Hash === map
        @value = {}
        map.each_pair do |key, value|
          key = key.to_sym
          @value[key] = normalize_map_property(key, value)
        end
      end

      def normalize_map_property(key, value)
        case @name
        when :compaction
          case key
          when :class
            value.sub(/^org\.apache\.cassandra\.db\.compaction\./, '')
          when :bucket_high, :bucket_low, :tombstone_threshold then value.to_f
          when :max_threshold, :min_threshold, :min_sstable_size,
            :sstable_size_in_mb, :tombstone_compaction_interval then value.to_i
          else value.to_s
          end
        when :compression
          case key
          when :sstable_compression
            value.sub(/^org\.apache\.cassandra\.io\.compress\./, '')
          when :chunk_length_kb then value.to_i
          when :crc_check_chance then value.to_f
          else value.to_s
          end
        end
      end

    end

  end

end
