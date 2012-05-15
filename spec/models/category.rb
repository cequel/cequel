class Category

  include Cequel::Model
  include Cequel::Model::Dynamic

  key :id, :int
  column :name, :text

end
