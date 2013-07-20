require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::List do
  model :Post do
    key :permalink, :text
    column :title, :text
    list :tags, :text
  end

  let(:scope) { cequel[:posts].where(:permalink => 'cequel') }
  subject { scope.first }

  let! :post do
    Post.new do |post|
      post.permalink = 'cequel'
      post.tags = %w(one two)
    end.tap(&:save)
  end

  let! :unloaded_post do
    Post['cequel']
  end

  context 'new record' do
    it 'should save list as-is' do
      subject[:tags].should == %w(one two)
    end
  end

  describe '#<<' do
    it 'should add new items' do
      post.tags << 'three' << 'four'
      post.save
      subject[:tags].should == %w(one two three four)
    end

    it 'should add new items atomically' do
      scope.list_append(:tags, 'three')
      post.tags << 'four' << 'five'
      post.save
      subject[:tags].should == %w(one two three four five)
    end

    it 'should add new items without reading' do
      unloaded_post.tags << 'four' << 'five'
      unloaded_post.save
      unloaded_post.should_not be_loaded
      subject[:tags].should == %w(one two four five)
    end
  end

  describe '#[]=' do
    before { scope.list_append(:tags, 'three') }

    it 'should atomically replace a single element' do
      post.tags[1] = 'TWO'
      post.save
      subject[:tags].should == %w(one TWO three)
      post.tags.should == %w(one TWO)
    end

    it 'should replace an element without reading' do
      cequel.should_not_receive :execute
      unloaded_post.tags[1] = 'TWO'
    end

    it 'should persist the replaced element' do
      unloaded_post.tags[1] = 'TWO'
      unloaded_post.save
      subject[:tags].should == %w(one TWO three)
    end

    it 'should atomically replace a given number of arguments' do
      post.tags[0, 2] = 'One', 'Two'
      post.save
      subject[:tags].should == %w(One Two three)
      post.tags.should == %w(One Two)
    end

    it 'should remove elements beyond positional arguments' do
      scope.list_append(:tags, 'four')
      post.tags[0, 3] = 'ONE'
      post.save
      subject[:tags].should == %w(ONE four)
      post.tags.should == %w(ONE)
    end

    it 'should atomically replace a given range of elements' do
      post.tags[0..1] = ['One', 'Two']
      post.save
      subject[:tags].should == %w(One Two three)
      post.tags.should == %w(One Two)
    end

    it 'should remove elements beyond positional arguments' do
      scope.list_append(:tags, 'four')
      post.tags[0..2] = 'ONE'
      post.save
      subject[:tags].should == %w(ONE four)
      post.tags.should == %w(ONE)
    end
  end

  describe '#clear' do
    it 'should clear all elements from the array' do
      post.tags.clear
      post.save
      subject[:tags].should be_blank
      post.tags.should == []
    end

    it 'should clear elements without loading' do
      cequel.should_not receive(:execute)
      unloaded_post.tags.clear
    end

    it 'should persist clear without loading' do
      unloaded_post.tags.clear
      unloaded_post.save
      subject[:tags].should be_blank
    end
  end

  describe '#collect!' do
    it 'should not respond' do
      expect { post.tags.collect!(&:upcase) }.to raise_error(NoMethodError)
    end
  end

  describe '#concat' do
    it 'should atomically concatenate elements' do
      scope.list_append(:tags, 'three')
      post.tags.concat(['four', 'five'])
      post.save
      subject[:tags].should == %w(one two three four five)
      post.tags.should == %w(one two four five)
    end

    it 'should concat elements without loading' do
      cequel.should_not_receive :execute
      unloaded_post.tags.concat(['four', 'five'])
    end

    it 'should persist concatenated elements' do
      unloaded_post.tags.concat(['four', 'five'])
      unloaded_post.save
      subject[:tags].should == %w(one two four five)
    end
  end

  describe '#delete' do
    it 'should atomically delete all instances of an object' do
      scope.list_append(:tags, 'three')
      scope.list_append(:tags, 'two')
      post.tags.delete('two')
      post.save
      subject[:tags].should == %w(one three)
      post.tags.should == %w(one)
    end

    it 'should delete without loading' do
      cequel.should_not_receive :execute
      unloaded_post.tags.delete('two')
    end

    it 'should persist deletions without loading' do
      unloaded_post.tags.delete('two')
      unloaded_post.save
      subject[:tags].should == %w(one)
    end
  end

  describe '#delete_at' do
    it 'should atomically delete from a given index' do
      scope.list_append(:tags, ['three', 'four'])
      post.tags.delete_at(1)
      post.save
      subject[:tags].should == %w(one three four)
      post.tags.should == %w(one)
    end

    it 'should delete from a given index without reading' do
      cequel.should_not_receive :execute
      unloaded_post.tags.delete_at(1)
    end

    it 'should persist deletion from unloaded list' do
      unloaded_post.tags.delete_at(1)
      unloaded_post.save
      subject[:tags].should == %w(one)
    end
  end

  describe '#delete_if' do
    it 'should not respond' do
      expect { post.tags.delete_if { |tag| tag.start_with?('o') } }.
        to raise_error(NoMethodError)
    end
  end

  describe '#fill' do
    it 'should not respond' do
      expect { post.tags.fill('seventy') }.to raise_error(NoMethodError)
    end
  end

  describe '#flatten!' do
    it 'should not respond' do
      expect { post.tags.flatten! }.to raise_error(NoMethodError)
    end
  end

  describe '#insert' do
    it 'should not respond' do
      expect { post.tags.insert(4, 'five') }.to raise_error(NoMethodError)
    end
  end

  describe '#keep_if' do
    it 'should not respond' do
      expect { post.tags.keep_if { |e| e.start_with?('o') } }.
        to raise_error(NoMethodError)
    end
  end

  describe '#map!' do
    it 'should not respond' do
      expect { post.tags.map! { |e| e.upcase } }.
        to raise_error(NoMethodError)
    end
  end

  describe '#pop' do
    it 'should not respond' do
      expect { post.tags.pop }.to raise_error(NoMethodError)
    end
  end

  describe '#push' do
    it 'should add new items atomically' do
      scope.list_append(:tags, 'three')
      post.tags.push('four').push('five')
      post.save
      subject[:tags].should == %w(one two three four five)
      post.tags.should == %w(one two four five)
    end
  end

  describe '#reject!' do
    it 'should not respond' do
      expect { post.tags.reject! { |e| e.start_with?('o') } }.
        to raise_error(NoMethodError)
    end
  end

  describe '#replace' do
    it 'should just overwrite the whole array' do
      scope.list_append(:tags, 'three')
      post.tags.replace(%w(four five))
      post.save
      subject[:tags].should == %w(four five)
      post.tags.should == %w(four five)
    end

    it 'should overwrite without reading' do
      cequel.should_not_receive :execute
      unloaded_post.tags.replace(%w(four five))
    end

    it 'should persist unloaded overwrite' do
      unloaded_post.tags.replace(%w(four five))
      unloaded_post.save
      subject[:tags].should == %w(four five)
    end
  end

  describe '#reverse!' do
    it 'should not respond' do
      expect { post.tags.reverse! }.to raise_error(NoMethodError)
    end
  end

  describe '#rotate!' do
    it 'should not respond' do
      expect { post.tags.rotate! }.to raise_error(NoMethodError)
    end
  end

  describe '#select!' do
    it 'should not respond' do
      expect { post.tags.select! { |e| e.start_with?('o') } }.
        to raise_error(NoMethodError)
    end
  end

  describe '#shift' do
    it 'should not respond' do
      expect { post.tags.shift }.to raise_error(NoMethodError)
    end
  end

  describe '#shuffle!' do
    it 'should not respond' do
      expect { post.tags.shuffle! }.to raise_error(NoMethodError)
    end
  end

  describe '#slice!' do
    it 'should not respond' do
      expect { post.tags.slice!(1, 2) }.to raise_error(NoMethodError)
    end
  end

  describe '#sort!' do
    it 'should not respond' do
      expect { post.tags.sort! }.to raise_error(NoMethodError)
    end
  end

  describe '#sort_by!' do
    it 'should not respond' do
      expect { post.tags.sort_by! { |e| e.reverse } }.
        to raise_error(NoMethodError)
    end
  end

  describe '#uniq!' do
    it 'should not respond' do
      expect { post.tags.uniq! }.to raise_error(NoMethodError)
    end
  end

  describe '#unshift' do
    it 'should atomically unshift' do
      scope.list_prepend(:tags, 'zero')
      post.tags.unshift('minustwo', 'minusone')
      post.save
      subject[:tags].should == %w(minustwo minusone zero one two)
      post.tags.should == %w(minustwo minusone one two)
    end

    it 'should unshift without reading' do
      cequel.should_not_receive :execute
      unloaded_post.tags.unshift('minustwo', 'minusone')
    end

    it 'should persist unloaded unshift' do
      unloaded_post.tags.unshift('minustwo', 'minusone')
      unloaded_post.save
      subject[:tags].should == %w(minustwo minusone one two)
    end
  end
end
