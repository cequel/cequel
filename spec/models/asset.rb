class Asset
  include Cequel::Model

  key :id, :integer
  column :class_name, :ascii
  column :label, :varchar
end
