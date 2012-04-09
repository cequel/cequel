require File.expand_path('../asset', __FILE__)

class Photo < Asset
  column :url, :varchar
end
