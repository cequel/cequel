# -*- encoding : utf-8 -*-
require_relative 'spec_helper'

describe Cequel::Uuids do
  describe '#uuid' do
    specify { Cequel.uuid.is_a?(Cassandra::TimeUuid) }
    specify { Cequel.uuid != Cequel.uuid }
    specify { time = Time.now; Cequel.uuid(time).to_time == time }
    specify { time = DateTime.now; Cequel.uuid(time).to_time == time.to_time }
    specify { time = Time.zone.now; Cequel.uuid(time).to_time == time.to_time }
    specify { val = Cequel.uuid.value; Cequel.uuid(val).value == val }
    specify { str = Cequel.uuid.to_s; Cequel.uuid(str).to_s == str }

    it 'should not share instances of Cassandra::TimeUuid::Generator between threads' do
      original_generator = Cequel.send(:timeuuid_generator)
      other_thread_generator = Thread.new { Cequel.send(:timeuuid_generator) }.value

      expect(original_generator).kind_of?(Cassandra::TimeUuid::Generator)
      expect(other_thread_generator).kind_of?(Cassandra::TimeUuid::Generator)
      expect(original_generator).not_to be(other_thread_generator)
    end

    it 'should not share Cassandra::TimeUuid::Generator state between forked processes' do
      original_generator = Cequel.send(:timeuuid_generator)
      forked_generator = Parallel.map([nil], in_processes: 1) { Cequel.send(:timeuuid_generator) }.first

      expect(original_generator).kind_of?(Cassandra::TimeUuid::Generator)
      expect(forked_generator).kind_of?(Cassandra::TimeUuid::Generator)
      expect(original_generator.instance_values).not_to eq(forked_generator.instance_values)
    end

    it 'should not share Cassandra::TimeUuid::Generator state between forked processes that spawn threads' do
      original_generator = Cequel.send(:timeuuid_generator)

      forked_other_thread_generator, forked_main_thread_generator = Parallel.map([nil], in_processes: 1) do
        other_thread_generator = Thread.new { Cequel.send(:timeuuid_generator) }.value
        main_thread_generator = Cequel.send(:timeuuid_generator)
        [other_thread_generator, main_thread_generator]
      end.first

      expect(original_generator).kind_of?(Cassandra::TimeUuid::Generator)
      expect(forked_other_thread_generator).kind_of?(Cassandra::TimeUuid::Generator)
      expect(forked_main_thread_generator).kind_of?(Cassandra::TimeUuid::Generator)
      expect(original_generator.instance_values).not_to eq(forked_other_thread_generator.instance_values)
      expect(original_generator.instance_values).not_to eq(forked_main_thread_generator.instance_values)
      expect(forked_other_thread_generator.instance_values).not_to eq(forked_main_thread_generator.instance_values)
    end
  end

  describe '#uuid?' do
    specify { Cequel.uuid?(Cequel.uuid) }
    specify { !Cequel.uuid?(Cequel.uuid.to_s) }
    if defined? SimpleUUID::UUID
      specify { !Cequel.uuid?(SimpleUUID::UUID.new) }
    end
  end
end
