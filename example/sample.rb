require 'rubygems'
require 'mysql2wrapper'

db_yml_path = File::dirname(__FILE__) + '/../config/database.yml'
puts db_yml_path
# 第3引数は複数DB接続が無いならば指定不要
db_config = Batchbase::Mysql2Wrapper.config_from_yml(db_yml_path,'test','test01')

client = Batchbase::Mysql2Wrapper.new(db_config)
# クエリログがうざいなら
#client.output_query_log = false
client.query "SELECT * FROM hoges"
client.transaction do
  client.query 'SELECT * FROM hoges'
  raise 'error' # call ROLLBACK
end
