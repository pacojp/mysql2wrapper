
class Array
  def select_one_must(&proc)
    result = self.select(&proc)
    raise StandardError, 'no data selected' if result.size == 0
    raise StandardError, "multiple data selected(#{result.size} datas)" if result.size > 1
    result.first
  end
end

module Mysql2
  class Result
    def select_one_must(&proc)
      self.to_a.select_one_must(&proc)
    end
  end
end
