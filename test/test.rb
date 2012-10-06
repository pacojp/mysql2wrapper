# -*- coding: utf-8 -*-

$: << File.dirname(__FILE__)
require 'test_helper'
require 'test/unit'
require 'Fileutils'


#
# localhostにmysql2wrapper_testというデータベースを作成し
# rootのパスワードなしでアクセスできるようにしておいてください
#

class TestMysql2wrapper < Test::Unit::TestCase

  def setup
    Batchbase::LogFormatter.skip_logging
    client = get_client

    query = 'DROP TABLE IF EXISTS `hoges`'
    client.query query

    query = '
    CREATE TABLE IF NOT EXISTS `hoges` (
      `id` int(11) NOT NULL auto_increment,
      `value1` int(11) NOT NULL,
      `created_at` datetime NOT NULL,
      `updated_at` timestamp NULL default NULL on update CURRENT_TIMESTAMP,
      PRIMARY KEY  (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '
    client.query query
    client.close
  end

  def get_client
    db_yml_path = File::dirname(__FILE__) + '/../config/database.yml'
    db_config = Mysql2wrapper::Client.config_from_yml(db_yml_path,'test')
    Mysql2wrapper::Client.new(db_config)
  end


  def insert(client)
    query = "INSERT INTO hoges (value1,created_at)VALUES(#{Time.now.to_i},NOW())"
    client.query query
  end

  def count(client=get_client)
    client.count('hoges','id')
  end

  def test_count
    client = get_client
    assert_equal 0,client.count('hoges','id')
    query = "INSERT INTO hoges (value1,created_at)VALUES(#{Time.now.to_i},NOW())"
    client.query query
    assert_equal 1,client.count('hoges','id')
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
end
