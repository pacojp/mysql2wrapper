# -*- coding: utf-8 -*-

#
# trial space
#

$: << File.dirname(__FILE__)

require 'rubygems'
require 'test_helper'
require 'logger'

require 'mysql2wrapper'

db_yml_path = File::dirname(__FILE__) + '/../config/database.yml'
puts db_yml_path
# 第3引数は複数DB接続が無いならば指定不要
db_config = Mysql2wrapper::Client.config_from_yml(db_yml_path,'test')

client = Mysql2wrapper::Client.new(db_config)
query = 'DROP TABLE IF EXISTS `hoges`'
client.query(query)
query = '
CREATE TABLE IF NOT EXISTS `hoges` (
  `id` int(11) NOT NULL auto_increment,
  `v_int1` int(11) NOT NULL,
  `v_int2` int(11) NOT NULL,
  `v_int3` int(11),
  `v_str1` varchar(10) NOT NULL,
  `v_str2` varchar(10) NOT NULL,
  `v_str3` varchar(10) NOT NULL,
  `v_datetime1` datetime NOT NULL,
  `v_datetime2` datetime NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_at` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY  (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
'
client.query query

@i = 0
increment = proc{@i+=1}

# hashでインサート
hash = {
  :v_int1=>increment,
  :v_int2=>rand(10000),
  :v_int3=>nil,
  :v_str1=>'test',
  :v_str2=>'hoho',
  :v_str3=>"CONCAT('My','S','QL')".to_func,
  :v_datetime1=>Time.mktime(2011,12,3,4,5,6),
  :v_datetime2=>DateTime.new(2011,12,3,4,5,6),
  :created_at=>'NOW()'.to_func
}
client.insert('hoges',hash)

# hashの配列でマルチプルインサート
array = []
101.times{array << hash}
client.insert('hoges',array)
puts "hoge has #{client.count 'hoges'}rows"

client.transaction do
  client.insert('hoges',hash)
end
puts "hoge has #{client.count 'hoges'}rows"

begin
  client.transaction do
    client.insert('hoges',hash)
    raise 'roooooooooollback!'
  end
rescue
end
puts "hoge has #{client.count 'hoges'}rows"

client.update 'hoges',{:v_int1=>2},'id = 1'
client.update 'hoges',{:v_int1=>2},client.class::UPDATE_ALL

