require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record::KeyCollisions do
  model :BlogWithDuplicateKeyOverwrite do
    key :subdomain, :text
    column :name, :text
    on_duplicate_key :overwrite
  end

  model :BlogWithDuplicateKeyError do
    key :subdomain, :text
    column :name, :text
    on_duplicate_key :error
  end

  model :BlogWithDuplicateKeyIgnore do
    key :subdomain, :text
    column :name, :text
    on_duplicate_key :ignore
  end

  describe 'on_duplicate_key :overwrite' do
    let!(:blog) do
      BlogWithDuplicateKeyOverwrite.create!(
        subdomain: 'cassandra', name: 'Cassandra')
    end

    it 'should not raise error on duplicate key' do
      expect do
        BlogWithDuplicateKeyOverwrite.create!(
          subdomain: 'cassandra',
          name: 'Cassandra Blog')
      end.to_not raise_error

      expect(BlogWithDuplicateKeyOverwrite.find('cassandra').name)
        .to eq('Cassandra Blog')
    end
  end

  describe 'on_duplicate_key :error' do
    let!(:blog) do
      BlogWithDuplicateKeyError.create!(
        subdomain: 'cassandra', name: 'Cassandra')
    end

    it 'should raise error on duplicate key' do
      expect do
        BlogWithDuplicateKeyError.create!(
          subdomain: 'cassandra',
          name: 'Cassandra Blog')
      end.to raise_error(Cequel::Record::DuplicateKey)

      expect(BlogWithDuplicateKeyError.find('cassandra').name)
        .to eq('Cassandra')
    end
  end

  describe 'on_duplicate_key :ignore' do
    let!(:blog) do
      BlogWithDuplicateKeyIgnore.create!(
        subdomain: 'cassandra', name: 'Cassandra')
    end

    it 'should not raise error on duplicate key' do
      expect do
        BlogWithDuplicateKeyIgnore.create!(
          subdomain: 'cassandra', name: 'Cassandra Blog')
      end.to_not raise_error

      expect(BlogWithDuplicateKeyIgnore.find('cassandra').name)
        .to eq('Cassandra')
    end
  end
end
