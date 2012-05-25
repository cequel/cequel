class Asset

  include Cequel::Model

  key :id, :integer
  column :class_name, :ascii
  column :label, :varchar
  column :checksum, :varchar

  index_preference :checksum, :class_name

  def observed!(callback)
    @observed ||= []
    @observed << callback
  end

  def has_observed?(callback)
    @observed.include?(callback)
  end

end
