class Asset

  include Cequel::Model

  key :id, :integer
  column :class_name, :ascii
  column :label, :varchar

  def observed!(callback)
    @observed ||= []
    @observed << callback
  end

  def has_observed?(callback)
    @observed.include?(callback)
  end

end
