# -*- encoding : utf-8 -*-
require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record do
  model :Post do
    connection.execute("CREATE TYPE cequel_test.person (name text, last_name text, age int)")
    key :permalink, :text
    column :title, :text
    udt :author, :person
  end

  let(:scope) { cequel[Post.table_name].where(permalink: 'cequel') }
  subject { scope.first }
  let(:author) { { name: 'Foo', last_name: 'Bar', age: 25 } }
  let(:new_author) { { name: 'Bar', last_name: 'Foo', age: 26 } }

  let! :post do
    Post.new do |post|
      post.permalink = 'cequel'
      post.author = author
    end.tap(&:save)
  end

  let! :unloaded_post do
    Post['cequel']
  end

  context 'new record' do
    it 'should save the UDT as is' do
      expect(subject[:author]).to eq(Cassandra::UDT.new(author))
    end
  end

  context 'updating' do
    it 'should overwrite value' do
      post.author = new_author
      post.save!
      expect(subject[:author]).to eq(Cassandra::UDT.new(new_author))
      expect(post.author).to eq(new_author)
    end

    it 'should cast collection before overwriting' do
      post.author = new_author.to_a
      post.save!
      expect(subject[:author]).to eq(Cassandra::UDT.new(new_author))
      expect(post.author).to eq(new_author)
    end
  end

  describe 'atomic modification' do
    describe '#[]=' do
      it 'should atomically update' do
        pending 'TODO: Implement UDT atomic modification'
        post.author['age'] = 41
        post.save
        expect(subject[:author]).to eq(author.merge(age: 41))
        expect(post.author).to eq(author.merge(age: 41))
      end

      it 'should write without reading' do
        pending 'TODO: Implement UDT atomic modification'
        expect_statement_count 1 do
          unloaded_post.author['age'] = 41
          unloaded_post.save
        end
        expect(subject[:author]).to eq(author.merge(age: 41))
      end
    end
  end
end
