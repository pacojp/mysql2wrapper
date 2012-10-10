require "mysql2wrapper/version"
require "mysql2wrapper/client"

class String
  def to_func
    @__function_sql = true
    self
  end

  def function_sql?
    @__function_sql == true
  end
end

module Mysql2wrapper
end
