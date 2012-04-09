class Asset
  include Cequel::Model

  key :id, :integer
  column :type, :ascii
  column :label, :varchar
end
