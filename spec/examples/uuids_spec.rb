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

  describe "uuid conversion" do
    model :Mention do
      key    :id,                   :timeuuid, auto: true
      column :keyword_id,           :int, partition: true # the keyword id of the mention
    
      column :profile_id,           :text, index: true
      column :profile_name,         :text
      column :profile_image_url,    :text
      column :profile_url,          :text
    
      column :source_url,           :text                  # the permalink for the mention
      column :source,               :text                  # where did we get it, customer showable
      column :retriever_type,       :text                  # what retrieving process found this
      column :content_type,         :text
      column :keyword,              :text, index: true     # the name of the keyword
      column :lang,                 :text                  # the 2 letter code for the language
      column :title,                :text                  # the page title if a web page
      column :text,                 :text                  # the text found on the mention
      column :snippet,              :text
      timestamps
    end

    # Gives us away to see mentions by keyword_id in descending order
    model :TrackMentions do
      key    :keyword_id, :int
      key    :ts,             :timestamp, order: :desc
      column :mention_id,     :timeuuid

      class << self
        def add_to_list(keyword_id, mention_id)
          new(
            keyword_id: keyword_id,
            mention_id: mention_id,
            ts: Time.now.utc
          ).save
        end
      end
    end

    let(:keyword_id) { [1,2,3,4].sample }
    let(:column_list) do
      %w[profile_id profile_name profile_image_url profile_url 
        source_url source retriever_type content_type keyword lang 
        title text snippet]
    end

    it "should not error" do
      expect do
        puts "Building data..."
        1000.times do
          mention = Mention.new
          mention.keyword_id = keyword_id
          column_list.each do |column_name|
            mention.send("#{column_name}=", SecureRandom.base64)
          end
          expect(mention.save).to be_truthy
          TrackMentions.add_to_list(keyword_id, mention.id)
        end
        
        puts "Performing tests..."
        # Retreive teh stored mention_ids from another model
        mention_ids = TrackMentions.all.map { |record| record.mention_id }
        
        # mention_ids.pop if interval == 9 (adding this fixes the problem because it makes it 999 entries)
        puts "Mention_ids is #{mention_ids.length} long"
        mentions = Mention.where(id: mention_ids).all.to_a

        # Now we iterate over them to make sure they're not busted
        mentions.each do |mention|
          column_list.each do |column_name|
            expect(mention.send(column_name)).to be_present
          end
        end
      end.to_not raise_error
    end
  end
end
