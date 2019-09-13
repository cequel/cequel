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
  end

  describe '#uuid?' do
    specify { Cequel.uuid?(Cequel.uuid) }
    specify { !Cequel.uuid?(Cequel.uuid.to_s) }
    if defined? SimpleUUID::UUID
      specify { !Cequel.uuid?(SimpleUUID::UUID.new) }
    end
  end
end
