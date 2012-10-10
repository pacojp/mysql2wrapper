require 'rubygems'
require 'mysql2wrapper'
require 'logger'

logger = Logger.new(STDOUT)
# フォーマッターを変える
#logger.formatter = Kanamei::LogFormatter.formatter
db_yml_path = File::dirname(__FILE__) + '/../config/database.yml'
logger.info db_yml_path

# 第3引数は複数DB接続が無いならば指定不要
db_config = Mysql2wrapper::Client.config_from_yml(db_yml_path,'test')

client = Mysql2wrapper::Client.new(db_config,logger)
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;'
client.query query

# クエリログがうざいなら
#client.output_query_log = false
#
logger.info "hoge has #{client.count 'hoges'}rows"
client.query "INSERT INTO hoges (value1,created_at)VALUES(#{Time.now.to_i},NOW())"
logger.info "hoge has #{client.count 'hoges'}rows"

begin
  client.transaction do
    logger.info "hoge has #{client.count 'hoges'}rows"
   client.query "INSERT INTO hoges (value1,created_at)VALUES(#{Time.now.to_i},NOW())"
    client.query query
    logger.info "hoge has #{client.count 'hoges'}rows"
    raise 'error' # call ROLLBACK
  end
rescue
end

logger.info "hoge has #{client.count 'hoges'}rows"
@i = 0
hash = {
  :value1=>proc{@i+=1}, # procを引数にもできる
  :created_at=>'now()'.to_func
}
# ハッシュを引数にしたインサートの発行
client.insert('hoges',hash)
logger.info "hoge has #{client.count 'hoges'}rows"

ar = []
20.times{ar << hash}
# 配列を引数にしたインサートの発行
client.insert('hoges',hash)
client.insert('hoges',ar)

logger.info "hoge has #{client.count 'hoges'}rows"
