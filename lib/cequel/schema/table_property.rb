# -*- encoding : utf-8 -*-
module Cequel
  module Schema
    #
    # Encapsulates a CQL3 storage property defined on a table
    #
    class TableProperty
      # @return [Symbol] name of the property
      attr_reader :name
      # @return value of the property
      attr_reader :value

      #
      # Initialize an instance of the appropriate TableProperty implementation.
      #
      # @param (see #initialize)
      # @api private
      #
      def self.build(name, value)
        clazz =
          case name.to_sym
          when :compaction then CompactionProperty
          when :compression then CompressionProperty
          else TableProperty
          end
        clazz.new(name, value)
      end

      #
      # @param name [Symbol] name of the property
      # @param value value of the property
      #
      def initialize(name, value)
        @name = name
        self.normalized_value = value
      end
      class << self; protected :new; end

      #
      # @return [String] CQL fragment defining this property in a `CREATE
      #   TABLE` statement
      #
      def to_cql
        %("#{@name}" = #{value_cql})
      end

      protected

      def normalized_value=(value)
        @value = value
      end

      private

      def value_cql
        quote(@value)
      end

      def quote(value)
        Cequel::Type.quote(value)
      end
    end

    #
    # A table property whose value is itself a map of keys and values
    #
    # @abstract Inheriting classes must implement
    #   `#normalize_map_property(key, value)`
    #
    class MapProperty < TableProperty
      protected

      def normalized_value=(map)
        @value = {}
        map.each_pair do |key, value|
          key = key.to_sym
          @value[key] = normalize_map_property(key, value)
        end
      end

      private

      def value_cql
        map_pairs = @value.each_pair
          .map { |key, value| "#{quote(key.to_s)} : #{quote(value)}" }
          .join(', ')
        "{ #{map_pairs} }"
      end
    end

    #
    # A property comprising key-value pairs of compaction settings
    #
    class CompactionProperty < MapProperty
      private

      def normalize_map_property(key, value)
        case key
        when :class
          value.sub(/^org\.apache\.cassandra\.db\.compaction\./, '')
        when :bucket_high, :bucket_low, :tombstone_threshold then value.to_f
        when :max_threshold, :min_threshold, :min_sstable_size,
          :sstable_size_in_mb, :tombstone_compaction_interval then value.to_i
        else value.to_s
        end
      end
    end

    #
    # A property comprising key-value pairs of compression settings
    #
    class CompressionProperty < MapProperty
      private

      def normalize_map_property(key, value)
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
