# -*- coding: utf-8 -*-

$: << File.dirname(__FILE__)
require 'test_helper'
require 'test/unit'
require 'Fileutils'
require 'logger'

#
# localhostに以下のデータベースを作成しrootのパスワード無しでアクセスできるようにしておいてください
#
# mysql2wrapper_test
# mysql2wrapper_test_master
# mysql2wrapper_test_slave
#

class TestMysql2wrapper < Test::Unit::TestCase

  def setup
    @i = 0
    client = get_client

    query = 'DROP TABLE IF EXISTS `test01`'
    client.query query
    query = 'DROP TABLE IF EXISTS `test02`'
    client.query query

    query = simple_table_create_query('test01')
    client.query query

    query = '
CREATE TABLE IF NOT EXISTS `test02` (
  `id` int(11) NOT NULL auto_increment,
  `v_int1` int(11) NOT NULL,
  `v_int2` int(11) NOT NULL,
  `v_int3` int(11),
  `v_int4` int(11),
  `v_bool1` tinyint(1) NOT NULL,
  `v_bool2` tinyint(1) NOT NULL,
  `v_bool3` tinyint(1),
  `v_bool4` tinyint(1),
  `v_str1` varchar(10) NOT NULL,
  `v_str2` varchar(10) NOT NULL,
  `v_str3` varchar(10),
  `v_str4` varchar(10),
  `v_date1` date NOT NULL,
  `v_date2` date,
  `v_date3` date,
  `v_datetime1` datetime NOT NULL,
  `v_datetime2` datetime,
  `v_datetime3` datetime,
  `v_time1` datetime NOT NULL,
  `v_time2` datetime,
  `v_time3` datetime,
  `created_at` datetime NOT NULL,
  `updated_at` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY  (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '
    client.query query
    client.close
  end

  def get_client
    db_yml_path = File::dirname(__FILE__) + '/../config/database.yml'
    db_config = Mysql2wrapper::Client.config_from_yml(db_yml_path,'test')
    client = Mysql2wrapper::Client.new(db_config,nil)
    client
  end

  def simple_table_create_query(table)
    "
CREATE TABLE IF NOT EXISTS `#{table}` (
  `id` int(11) NOT NULL auto_increment,
  `v_int1` int(11) NOT NULL,
  `v_int2` int(11),
  `v_str1` varchar(10) ,
  `created_at` datetime NOT NULL,
  `updated_at` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY  (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    "
  end

  def hash_for_simple_table
    {
      :v_int1     => proc{ @i+=1 },
      :created_at => 'NOW()'.to_func,
    }
  end

  def insert(client)
    query = "INSERT INTO test01 (v_int1,created_at)VALUES(#{Time.now.to_i},NOW())"
    client.query query
  end

  def count(client=get_client)
    client.count('test01','id')
  end

  def test_to_func
    assert_equal false,''.function_sql?
    assert_equal true,''.to_func.function_sql?
    st = ""
    assert_equal false,st.function_sql?
    st = st.to_func
    assert_equal true,st.function_sql?
    assert_equal true,proc{"".to_func}.call.function_sql?
    assert_equal true,Proc.new{"".to_func}.call.function_sql?
  end

  def test_count
    client = get_client
    assert_equal 0,client.count('test01',nil,'id')
    assert_equal 0,client.count('test01')
    client.insert('test01',{:v_int1=>11,:created_at=>'NOW()'.to_func})
    assert_equal 1,client.count('test01')
    client.insert('test01',{:v_int1=>11,:created_at=>'NOW()'.to_func})
    assert_equal 2,client.count('test01',nil,'id')
    assert_equal 2,client.count('test01',nil,'*')
    assert_equal 2,client.count('test01')
    client.insert('test01',{:v_int1=>11,:v_int2=>22,:created_at=>'NOW()'.to_func})
    assert_equal 3,client.count('test01',{:v_int1=>11},'id')
    assert_equal 3,client.count('test01',"v_int1 = 11",'id')
    assert_equal 1,client.count('test01',{:v_int2=>22},'id')
    assert_equal 1,client.count('test01',{:v_int1=>11,:v_int2=>22},'id')
    assert_equal 1,client.count('test01',{:v_int1=>11,:v_int2=>22})
    assert_equal 1,client.count('test01',"v_int1 = 11 AND v_int2 = 22",'id')
    client.close
  end

  def test_last_query
    client = get_client
    query = "INSERT INTO test01 (v_int1,created_at)VALUES(123,NOW())"
    client.query query
    assert_equal query,client.last_query
    begin
      client.transaction do
        client.query query
        assert_equal query,client.last_query
        raise 'hoho'
      end
    rescue => e
      assert_equal query,client.last_query
    end
  end

  def test_select
    client = get_client
    assert_equal 0,client.count('test01','id')
    query = "INSERT INTO test01 (v_int1,created_at)VALUES(123,NOW())"
    client.query query
    assert_equal 1,client.count('test01','id')
    res = client.query "SELECT * FROM test01 WHERE id = 1"
    assert_equal 123, res.first['v_int1']
    assert_equal 1, client.affected_rows
    query = "INSERT INTO test01 (v_int1,created_at)VALUES(123,NOW())"
    client.query query
    res = client.query "SELECT * FROM test01 WHERE id = 1"
    assert_equal 1, client.affected_rows
    res = client.select "test01",'*',{:id=>1}
    assert_equal 1, client.affected_rows
    res = client.query "SELECT * FROM test01"
    assert_equal 2, client.affected_rows

    query = "INSERT INTO test01 (v_int1,v_int2,created_at)VALUES(123,234,NOW())"
    client.query query
    res = client.select "test01",'id',{:v_int1=>123,:v_int2=>234}
    assert_equal 1, client.affected_rows
    assert_equal 1, res.size
    res = client.select "test01",'*',"v_int1 =123 AND v_int2 = 234"
    assert_equal 1, client.affected_rows
    assert_equal 1, res.size
    res = client.select "test01",'id',{:v_int1=>123,:v_int2=>nil}
    assert_equal 2, client.affected_rows
    res = client.select "test01",'*',"v_int1=123 AND v_int2 IS NULL"
    assert_equal 2, client.affected_rows
    client.close
  end

  def test_update
    client = get_client
    query = "INSERT INTO test01 (v_int1,created_at)VALUES(1,NOW())"
    client.query query
    client.query query
    client.query query

    client.update 'test01',{:v_int1=>2},'id = 1'
    assert_equal 1, client.affected_rows
    res = client.query "SELECT * FROM test01 WHERE id = 1"
    assert_equal 2, res.first['v_int1']
    assert_equal nil, res.first['v_int2']
    res = client.query "SELECT * FROM test01 WHERE id = 2"
    assert_equal 1, res.first['v_int1']
    assert_equal nil, res.first['v_int2']

    client.update 'test01',{:v_int2=>3},{:id=>3}
    assert_equal 1, client.affected_rows
    client.update 'test01',{:v_int1=>3}, client.update_all_flag

    assert_equal 3, client.affected_rows
    res = client.query "SELECT * FROM test01 WHERE id = 1"
    assert_equal 3, res.first['v_int1']
    assert_equal nil, res.first['v_int2']
    res = client.query "SELECT * FROM test01 WHERE id = 2"
    assert_equal 3, res.first['v_int1']
    assert_equal nil, res.first['v_int2']
    client.update 'test01',{:v_int1=>3},Mysql2wrapper::Client::UPDATE_ALL
    assert_equal 0, client.affected_rows # 更新行が無いので

    client.update 'test01',{:v_int1=>4},{:v_int1=>2,:v_int2=>nil}
    assert_equal 0, client.affected_rows
    client.update 'test01',{:v_int1=>4},{:v_int1=>3,:v_int2=>nil}
    assert_equal 2, client.affected_rows
    client.update 'test01',{:v_int1=>4},{:v_int1=>3,:v_int2=>3}
    assert_equal 1, client.affected_rows

    client.update 'test01',{:v_int1=>5},client.update_all_flag
    assert_equal 3, client.affected_rows

    client.close
  end

  def test_update_all_limitter
    client = get_client
    query = "INSERT INTO test01 (v_int1,created_at)VALUES(1,NOW())"
    client.query query

    assert_raise(ArgumentError){
      client.update 'test01',{:v_int1=>3},nil
    }
    assert_raise(ArgumentError){
      client.update 'test01',{:v_int1=>3},''
    }

    assert_raise(Mysql2::Error){
      client.update 'test01',{:v_int5=>3},Mysql2wrapper::Client::UPDATE_ALL
    }
  end

  def test_escape
    client = get_client
    assert_equal 0,client.count('test01','id')
    query = "INSERT INTO test01 (v_int1,v_str1,created_at)VALUES(#{Time.now.to_i},'#{client.escape("'te'st'st'")}',NOW())"
    client.query query
    res = client.query "SELECT * FROM test01"
    assert_equal 1,res.size
    assert_equal "'te'st'st'",res.first['v_str1']
    client.close
  end

  def test_tranasction_simple
    client = get_client
    insert(client)
    assert_equal 1,count(client)
    begin
      client.transaction do
        insert(client)
        assert_equal 2,count(client)
        raise 'roooooooooollback!!!!'
      end
    rescue
    end
    assert_equal 1,count(client)
    insert(client)
    assert_equal 2,count(client)
    client.close
  end

  def test_tranasction_2_clients
    client1 = get_client
    client2 = get_client
    insert(client1)
    assert_equal 1,count(client1)
    assert_equal 1,count(client2)
    begin
      # コネクションはトランザクション単位
      client1.transaction do
        insert(client1)
        assert_equal 2,count(client1)
        assert_equal 1,count(client2)
        raise 'roooooooooollback!!!!'
      end
    rescue
    end
    assert_equal 1,count(client1)
    assert_equal 1,count(client2)

    client1.transaction do
      insert(client2) #  raiseしないなら
    end
    assert_equal 2,count(client1)
    assert_equal 2,count(client2)

    begin
      # コネクションはトランザクション単位
      # client1のtransactionブロック内でclient2のインサートを発行しても
      # もちろんロールバックされない
      client1.transaction do
        insert(client1)
        assert_equal 3,count(client1)
        assert_equal 2,count(client2)
        insert(client2)
        assert_equal 3,count(client1) # ここは3
        assert_equal 3,count(client2)
        raise 'roooooooooollback!!!!'
      end
    rescue
    end
    assert_equal 3,count(client1)
    assert_equal 3,count(client2)
    client1.close
    client2.close
  end

  def test_insert
    client = get_client
    @i = 0
    hash = {
      :v_int1 => proc{ @i+=1 },
      :v_int2 => 246,
      :v_int3 => nil,
      :v_str1=>"te'st日本語",
      :v_str2=>"CONCAT('My','S','QL')".to_func,
      :v_str3 => nil,
      :v_bool1 => true,
      :v_bool2 => false,
      :v_bool3 => nil,
      :v_date1 => Date.new(2000,1,2),
      :v_date2 => nil,
      :v_datetime1 => DateTime.new(2000,1,2,3,4,5),
      :v_datetime2 => nil,
      :v_time1 => Time.mktime(2000,1,2,3,4,5),
      :v_time2 => nil,
      :created_at => 'NOW()'.to_func,
    }
    client.insert('test02',hash)
    assert_equal 1, client.affected_rows
    client.insert('test02',hash)
    res = client.query ("SELECT * FROM test02")
    assert_equal 2, res.size
    row = res.to_a.last
    assert_equal 2,       row['v_int1']
    assert_equal 246,     row['v_int2']
    assert_equal nil,     row['v_int3']
    assert_equal nil,     row['v_int4']
    assert_equal 'te\'st日本語', row['v_str1']
    assert_equal 'MySQL', row['v_str2']
    assert_equal nil,     row['v_str3']
    assert_equal nil,     row['v_str4']
    assert_equal 1,       row['v_bool1']
    assert_equal 0,       row['v_bool2']
    assert_equal nil,     row['v_bool3']
    assert_equal nil,     row['v_bool4']
    assert_equal Date.new(2000,1,2),row['v_date1']
    assert_equal nil,     row['v_date2']
    assert_equal nil,     row['v_date3']
    assert_equal Time.mktime(2000,1,2,3,4,5),row['v_datetime1']
    assert_equal nil,     row['v_datetime2']
    assert_equal nil,     row['v_datetime3']
    assert_equal Time.mktime(2000,1,2,3,4,5),row['v_time1']
    assert_equal nil,     row['v_time2']
    assert_equal nil,     row['v_time3']
  end

  def test_multiple_insert
    client = get_client
    @i = 0
    hash = hash_for_simple_table
    client.insert('test01',hash)
    res = client.query ("SELECT * FROM test01")
    assert_equal 1, res.size
    assert_equal 1, client.affected_rows
    client.insert('test01',hash)
    res = client.query ("SELECT * FROM test01")
    assert_equal 2, res.size
    assert_equal 2, res.to_a.last['v_int1']
    ar = []
    111.times{ar << hash}
    client.insert('test01',ar)
    assert_equal 111, client.affected_rows
    res = client.query ("SELECT * FROM test01")
    assert_equal 113, res.size
    assert_equal 113, res.to_a.last['v_int1']
  end

  def test_sql_mode_is_strict_all_tables
    assert_raise(Mysql2::Error) do
      client = get_client
      query = "INSERT INTO test01 (v_int1)values(1)"
      client.query(query)
    end
  end

  def test_multiple_database
    db_yml_path   = File::dirname(__FILE__) + '/../config/database_multiple.yml'
    config_master = Mysql2wrapper::Client.config_from_yml(db_yml_path,'test','master')
    config_slave  = Mysql2wrapper::Client.config_from_yml(db_yml_path,'test','slave')
    client_master = Mysql2wrapper::Client.new(config_master,nil)
    client_slave  = Mysql2wrapper::Client.new(config_slave,nil)

    client_master.query 'DROP TABLE IF EXISTS tbl_master'
    client_master.query simple_table_create_query('tbl_master')
    client_slave.query  'DROP TABLE IF EXISTS tbl_slave'
    client_slave.query  simple_table_create_query('tbl_slave')

    assert_equal 0,client_master.count('tbl_master')
    assert_equal 0,client_slave.count('tbl_slave')

    client_master.insert('tbl_master',hash_for_simple_table)

    assert_equal 1,client_master.count('tbl_master')
    assert_equal 0,client_slave.count('tbl_slave')

    client_master.insert('tbl_master',hash_for_simple_table)

    assert_equal 2,client_master.count('tbl_master')
    assert_equal 0,client_slave.count('tbl_slave')

    client_slave.insert('tbl_slave',hash_for_simple_table)

    assert_equal 2,client_master.count('tbl_master')
    assert_equal 1,client_slave.count('tbl_slave')
  end

  # TODO
  def test_message_for_database_yaml_error
  end

  def test_tables
    client = get_client
    assert_equal 2, client.tables.size
    client.tables.each do |table_name|
      assert %w|test01 test02|.include?(table_name)
    end
  end

  # TODO
  def test_table_names
    client = get_client
    assert_equal 2, client.tables.size
    client.tables.each do |table_name|
      assert %w|test01 test02|.include?(table_name)
    end
  end

  def test_config_from_yml
  end

  # TODO
  def test_table_infomations
    client = get_client
    table_informations = client.table_informations
    assert_equal 2, table_informations.size
    table_informations.each do |hash|
      assert %w|test01 test02|.include?(hash['TABLE_NAME'])
    end
  end
end
