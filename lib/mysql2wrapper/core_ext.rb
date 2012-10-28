
class Array
  def select_one_must(&proc)
    result = self.select(&proc)
    raise StandardError, 'no data selected' if result.size == 0
    raise StandardError, "multiple data selected(#{result.size} datas)" if result.size > 1
    result.first
  end
end
