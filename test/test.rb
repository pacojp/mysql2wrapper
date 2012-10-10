# -*- coding: utf-8 -*-

$: << File.dirname(__FILE__)
require 'test_helper'
require 'test/unit'
require 'Fileutils'
require 'logger'


#
# localhostにmysql2wrapper_testというテーブルを作成し
# rootのパスワードなしでアクセスできるようにしておいてください
#

class TestMysql2wrapper < Test::Unit::TestCase

  def setup
    client = get_client

    query = 'DROP TABLE IF EXISTS `test01`'
    client.query query
    query = 'DROP TABLE IF EXISTS `test02`'
    client.query query

    query = '
CREATE TABLE IF NOT EXISTS `test01` (
  `id` int(11) NOT NULL auto_increment,
  `v_int1` int(11) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_at` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  PRIMARY KEY  (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '
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
    client = Mysql2wrapper::Client.new(db_config,Logger.new('/dev/null'))
    client
  end

  def insert(client)
    query = "INSERT INTO test01 (v_int1,created_at)VALUES(#{Time.now.to_i},NOW())"
    client.query query
  end

  def count(client=get_client)
    client.count('test01','id')
  end

  def test_count
    client = get_client
    assert_equal 0,client.count('test01','id')
    query = "INSERT INTO test01 (v_int1,created_at)VALUES(#{Time.now.to_i},NOW())"
    client.query query
    assert_equal 1,client.count('test01','id')
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

  # TODO
  def test_insert
    client = get_client
    @i = 0
    hash = {
      :v_int1 => proc{ @i+=1 },
      :v_int2 => 246,
      :v_int3 => nil,
      :v_str1=>"te'st",
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
    client.insert('test02',hash)
    res = client.query ("SELECT * FROM test02")
    assert_equal 2, res.size
    row = res.to_a.last
    assert_equal 2,       row['v_int1']
    assert_equal 246,     row['v_int2']
    assert_equal nil,     row['v_int3']
    assert_equal nil,     row['v_int4']
    assert_equal 'te\'st',  row['v_str1']
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
    hash = {
      :v_int1     => proc{ @i+=1 },
      :created_at => 'NOW()'.to_func,
    }
    client.insert('test01',hash)
    res = client.query ("SELECT * FROM test01")
    assert_equal 1, res.size
    client.insert('test01',hash)
    res = client.query ("SELECT * FROM test01")
    assert_equal 2, res.size
    assert_equal 2, res.to_a.last['v_int1']
    ar = []
    111.times{ar << hash}
    client.insert('test01',ar)
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
end
