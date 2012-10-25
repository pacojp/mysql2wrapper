require 'rubygems'
require 'mysql2wrapper'
require 'logger'
require 'pp'

#
# HACKME サンプルをちょっとはマシに書く、、、、
#


logger = Logger.new(STDOUT)
# フォーマッターを変える
#logger.formatter = Kanamei::LogFormatter.formatter
db_yml_path = File::dirname(__FILE__) + '/../config/database.yml'
logger.info db_yml_path

# 第3引数は複数DB接続が無いならば指定不要
db_config = Mysql2wrapper::Client.config_from_yml(db_yml_path,'test')

client = Mysql2wrapper::Client.new(db_config,logger)
# ログ出力しないなら第二引数をnilで
#client = Mysql2wrapper::Client.new(db_config,nil)

query = '
DROP TABLE IF EXISTS `test01`'
client.query query
query = '
CREATE TABLE IF NOT EXISTS `test01` (
  `id` int(11) NOT NULL auto_increment,
  `value1` int(11) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_at` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY  (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;'
client.query query
query = '
CREATE TABLE IF NOT EXISTS `test02` (
  `id` int(11) NOT NULL auto_increment,
  `value1` int(11) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_at` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY  (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;'
client.query query

p client.tables

logger.info "test01 has #{client.count 'test01'}rows"
client.query "INSERT INTO test01 (value1,created_at)VALUES(#{Time.now.to_i},NOW())"
logger.info "test01 has #{client.count 'test01'}rows"

begin
  # トランザクションはブロックを渡す（ARと同じく）
  client.transaction do
    logger.info "test01 has #{client.count 'test01'}rows"
   client.query "INSERT INTO test01 (value1,created_at)VALUES(#{Time.now.to_i},NOW())"
    client.query query
    logger.info "test01 has #{client.count 'test01'}rows"
    raise 'error' # call ROLLBACK
  end
rescue
end
logger.info "test01 has #{client.count 'test01'}rows"

# ハッシュを引数にしたインサートの発行
@i = 0
hash = {
  :value1=>proc{@i+=1}, # procを引数にもできる
  :created_at=>'now()'.to_func
}
client.insert('test01',hash)
logger.info "affected_rows:#{client.affected_rows}"
logger.info "test01 has #{client.count 'test01'}rows"

# 配列を引数にしたインサートの発行
ar = []
20.times{ar << hash}
client.insert('test01',ar)
logger.info "affected_rows:#{client.affected_rows}" # Client#affected_rowsでマルチプルインサートのトータルインサート数が取れる
logger.info "test01 has #{client.count 'test01'}rows"

# ハッシュを引数にしたアップデート
client.update 'test01',{:value1=>3},'id = 1'
logger.info "test01 has #{client.count 'test01'}rows"
# 全行アップデートをしたなら第三引数でMysql2wrapper::Client::UpdateAllClassを引数にしないとダメ
client.update 'test01',{:value1=>3},Mysql2wrapper::Client::UpdateAllClass
# clientインスタンスにショートカットあり
client.update 'test01',{:value1=>4},client.update_all_flag
logger.info "test01 has #{client.count 'test01'}rows"

pp client.table_informations
