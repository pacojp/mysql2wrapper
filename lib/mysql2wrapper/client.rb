# -*- coding: utf-8 -*-

require 'yaml'
require 'batchbase'
require 'mysql2'

class Mysql2wrapper::Client
  attr_accessor :client, :logger

  QUERY_BASE_COLOR    = 35
  QUERY_SPECIAL_COLOR = 31

  MULTIPLE_INSERT_DEFAULT = 100

  def initialize(config,_logger=nil)
    self.logger = _logger || Logger.new(STDOUT)
    self.logger.info "mysql2 client created with #{config.inspect}"
    self.client = Mysql2::Client.new(config)
    # サーバが古いので一応問題あるけど以下の方向で
    # http://kennyqi.com/archives/61.html
    self.class.query(self.client,"SET NAMES 'utf8'",self.logger)
    self.class.query(self.client,"SET SQL_AUTO_IS_NULL=0",self.logger)
    self.class.query(self.client,"SET SQL_MODE=STRICT_ALL_TABLES",self.logger)
  end

  # TODO 実行時間。更新項目数
  def self.query(client,str,logger=nil,color=QUERY_BASE_COLOR)
    s = Time.now
    ret = client.query(str)
    e = Time.now
    if logger
      # TODO too long sql shorten
      logger.info "[QUERY] "" \e[#{color}m (#{((e-s)*1000).round(2)}ms) #{str}\e[0m"
    end
    ret
  end

  def query(str,color=QUERY_BASE_COLOR)
    self.class.query(self.client,str,self.logger,color)
  end

  def transaction(&proc)
    raise ArgumentError, "No block was given" unless block_given?
    #query "SET AUTOCOMMIT=0;",QUERY_SPECIAL_COLOR
    query "BEGIN",QUERY_SPECIAL_COLOR
    begin
      yield
      query "COMMIT",QUERY_SPECIAL_COLOR
    rescue => e
      query "ROLLBACK",QUERY_SPECIAL_COLOR
      raise e
    end
  end

  def self.make_config_key_symbol(config)
    new_config = {}
    config.each do |key,value|
      new_config[key.to_sym] = value
    end
    config = new_config
  end

  def count(table_name,key_name='*')
    query("SELECT COUNT(#{self.client.escape(key_name)}) AS cnt FROM #{self.client.escape(table_name)}").first['cnt']
  end

  def close
    self.client.close if self.client
  end

  def insert(table_name,data,multiple_insert_by=MULTIPLE_INSERT_DEFAULT)
    query = "INSERT INTO `#{self.client.escape(table_name)}`"
    _datas = data.clone
    case _datas
    when Array
      ;
    when Hash
      _datas = [_datas]
    else
      raise ArgumentError, "data must be Array or Hash"
    end

    return nil if _datas.size == 0

    _datas.each_slice(multiple_insert_by).each do |rows|
      query = <<"EOS"
INSERT INTO `#{self.client.escape(table_name)}`
(#{rows.first.keys.map{|o|"`#{self.client.escape(o.to_s)}`"}.join(',')})
VALUES
#{
  rows.map do |row|
  "(#{
    row.map do |key,value|
      case value
      when Proc
        "'#{self.client.escape(value.call.to_s)}'"
      when nil
        "NULL"
      when TrueClass,FalseClass
        if value
          "'1'"
        else
          "'0'"
        end
      # TODO when datetime time
      when Time,DateTime
        "'#{value.strftime("%Y-%m-%d %H:%M:%S")}'"
      when Date
        "'#{value.strftime("%Y-%m-%d")}'"
      else
        s = value
        s = s.to_s unless s.kind_of?(String)
        if s.respond_to?(:function_sql?) && s.function_sql?
          "#{value.to_s}"
        else
          "'#{self.client.escape(value.to_s)}'"
        end
      end
    end.join(',')
  })"
  end.join(',')
}
EOS
      self.query(query)
    end
  end

  #
  # db_server_nameはDB名そのもの（複数DB対応）
  #
  def self.config_from_yml(yml_path,environment,db_server_name=nil)
    db_config = YAML.load_file(yml_path)[environment]
    if db_server_name
      db_config = self.make_config_key_symbol(db_config[db_server_name])
    else
      db_config = self.make_config_key_symbol(db_config)
    end
    db_config
  end
end
