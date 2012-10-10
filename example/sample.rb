require 'rubygems'
require 'mysql2wrapper'

db_yml_path = File::dirname(__FILE__) + '/../config/database.yml'
puts db_yml_path
# 第3引数は複数DB接続が無いならば指定不要
db_config = Mysql2wrapper::Client.config_from_yml(db_yml_path,'test')

client = Mysql2wrapper::Client.new(db_config)
query = '
DROP TABLE IF EXISTS `hoges`'
client.query query
query = '
CREATE TABLE IF NOT EXISTS `hoges` (
  `id` int(11) NOT NULL auto_increment,
  `value1` int(11) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_at` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY  (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
'
client.query query

# クエリログがうざいなら
#client.output_query_log = false
#
puts "hoge has #{client.count 'hoges'}rows"
client.query "INSERT INTO hoges (value1,created_at)VALUES(#{Time.now.to_i},NOW())"
puts "hoge has #{client.count 'hoges'}rows"

begin
  client.transaction do
    puts "hoge has #{client.count 'hoges'}rows"
   client.query "INSERT INTO hoges (value1,created_at)VALUES(#{Time.now.to_i},NOW())"
    client.query query
    puts "hoge has #{client.count 'hoges'}rows"
    raise 'error' # call ROLLBACK
  end
rescue
end

puts "hoge has #{client.count 'hoges'}rows"
