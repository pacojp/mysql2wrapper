require 'rubygems'
require 'mysql2wrapper'
require 'logger'
require 'pp'

#
# データベース内のテーブル情報を所定の書式で出力サンプル
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

client.table_informations.each do |table|
  puts "/*"
  puts "[table]"
  puts table['TABLE_NAME']
  puts table['TABLE_COMMENT']
  puts "[columns]"
  table['COLUMNS'].each do |col|
    puts "#{col['COLUMN_NAME']}:#{col['COLUMN_COMMENT']}"
  end
  puts "[indexes]"
  table['INDEXES'].each do |col|
    puts "#{col['INDEX_NAME']}:#{col['INDEX_COMMENT']}"
  end
  puts "[history]"
  puts "*/"
end

