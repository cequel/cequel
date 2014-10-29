# -*- encoding : utf-8 -*-
require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record::Set do
  model :Post do
    key :permalink, :text
    column :title, :text
    set :tags, :text
  end

  let(:scope) { cequel[Post.table_name].where(:permalink => 'cequel') }
  subject { scope.first }

  let! :post do
    Post.new do |post|
      post.permalink = 'cequel'
      post.tags = Set['one', 'two']
    end.tap(&:save)
  end

  let! :unloaded_post do
    Post['cequel']
  end

  context 'new record' do
    it 'should save set as-is' do
      expect(subject[:tags]).to eq(Set['one', 'two'])
    end
  end

  context 'updating' do
    it 'should overwrite value' do
      post.tags = Set['three', 'four']
      post.save!
      expect(subject[:tags]).to eq(Set['three', 'four'])
    end

    it 'should cast collection before overwriting' do
      post.tags = %w(three four)
      post.save!
      expect(subject[:tags]).to eq(Set['three', 'four'])
    end
  end

  describe 'atomic modification' do
    before { scope.set_add(:tags, 'three') }

    describe '#add' do
      it 'should add atomically' do
        post.tags.add('four')
        post.save
        expect(subject[:tags]).to eq(Set['one', 'two', 'three', 'four'])
        expect(post.tags).to eq(Set['one', 'two', 'four'])
      end

      it 'should cast before adding' do
        post.tags.add(4)
        expect(post.tags).to eq(Set['one', 'two', '4'])
      end

      it 'should add without reading' do
        expect_statement_count 1 do
          unloaded_post.tags.add('four')
          unloaded_post.save
        end
        expect(subject[:tags]).to eq(Set['one', 'two', 'three', 'four'])
      end

      it 'should apply add post-hoc' do
        unloaded_post.tags.add('four')
        expect(unloaded_post.tags).to eq(Set['one', 'two', 'three', 'four'])
      end
    end

    describe '#clear' do
      it 'should clear atomically' do
        post.tags.clear
        post.save
        expect(subject[:tags]).to be_blank
        expect(post.tags).to eq(Set[])
      end

      it 'should clear without reading' do
        expect_statement_count 1 do
          unloaded_post.tags.clear
          unloaded_post.save
        end
        expect(subject[:tags]).to be_blank
      end

      it 'should apply clear post-hoc' do
        unloaded_post.tags.clear
        expect(unloaded_post.tags).to eq(Set[])
      end
    end

    describe '#delete' do
      it 'should delete atomically' do
        post.tags.delete('two')
        post.save
        expect(subject[:tags]).to eq(Set['one', 'three'])
        expect(post.tags).to eq(Set['one'])
      end

      it 'should cast before deleting' do
        post.tags.delete(:two)
        expect(post.tags).to eq(Set['one'])
      end

      it 'should delete without reading' do
        expect_statement_count 1 do
          unloaded_post.tags.delete('two')
          unloaded_post.save
        end
        expect(subject[:tags]).to eq(Set['one', 'three'])
      end

      it 'should apply delete post-hoc' do
        unloaded_post.tags.delete('two')
        expect(unloaded_post.tags).to eq(Set['one', 'three'])
      end
    end

    describe '#replace' do
      it 'should replace atomically' do
        post.tags.replace(Set['a', 'b'])
        post.save
        expect(subject[:tags]).to eq(Set['a', 'b'])
        expect(post.tags).to eq(Set['a', 'b'])
      end

      it 'should cast before replacing' do
        post.tags.replace(Set[1, 2, :three])
        expect(post.tags).to eq(Set['1', '2', 'three'])
      end

      it 'should replace without reading' do
        expect_statement_count 1 do
          unloaded_post.tags.replace(Set['a', 'b'])
          unloaded_post.save
        end
        expect(subject[:tags]).to eq(Set['a', 'b'])
      end

      it 'should apply delete post-hoc' do
        unloaded_post.tags.replace(Set['a', 'b'])
        expect(unloaded_post.tags).to eq(Set['a', 'b'])
      end
    end

    specify { expect { post.tags.add?('three') }.to raise_error(NoMethodError) }
    specify { expect { post.tags.collect!(&:upcase) }.
      to raise_error(NoMethodError) }
    specify { expect { post.tags.delete?('two') }.to raise_error(NoMethodError) }
    specify { expect { post.tags.delete_if { |s| s.starts_with?('t') }}.
      to raise_error(NoMethodError) }
    specify { expect { post.tags.flatten! }.to raise_error(NoMethodError) }
    specify { expect { post.tags.keep_if { |s| s.starts_with?('t') }}.
      to raise_error(NoMethodError) }
    specify { expect { post.tags.map!(&:upcase) }.
      to raise_error(NoMethodError) }
    specify { expect { post.tags.reject! { |s| s.starts_with?('t') }}.
      to raise_error(NoMethodError) }
    specify { expect { post.tags.select! { |s| s.starts_with?('t') }}.
      to raise_error(NoMethodError) }
  end
end
