#include <iostream>
#include <leveldb/db.h>
#include <string>

#include "leveldb/write_batch.h"
int main() {
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;  // 如果数据库不存在则自动创建

  // 打开数据库（存储在 /tmp/testdb 目录）
  leveldb::Status status = leveldb::DB::Open(options, "./testdb", &db);
  if (!status.ok()) {
    std::cerr << "无法打开数据库: " << status.ToString() << std::endl;
    return 1;
  }

  // ----------------- 写入数据 -----------------
  leveldb::WriteOptions write_options;
  write_options.sync = false;  // 异步写入（更快，但可能丢失最后几条数据）

  // 插入键值对
  status = db->Put(write_options, "name", "Alice");
  status = db->Put(write_options, "age", "25");
  status = db->Put(write_options, "city", "New York");
  leveldb::WriteBatch batch;
  batch.Put("key1", "value1");
  batch.Put("key2", "value2");
  batch.Delete("key3");
  status = db->Write(leveldb::WriteOptions(), &batch);

  // ----------------- 读取数据 -----------------
  leveldb::ReadOptions read_options;
  std::string value;

  // 单键读取
  status = db->Get(read_options, "name", &value);
  if (status.ok()) {
    std::cout << "name: " << value << std::endl;  // 输出: name: Alice
  } else {
    std::cerr << "键 'name' 不存在: " << status.ToString() << std::endl;
  }

  // ----------------- 使用快照读取 -----------------
  const leveldb::Snapshot* snapshot = db->GetSnapshot();  // 创建快照
  read_options.snapshot = snapshot;

  // 模拟并发写入（不影响快照）
  db->Put(write_options, "name", "Bob");

  // 快照仍然看到旧数据
  db->Get(read_options, "name", &value);
  std::cout << "[快照] name: " << value
            << std::endl;  // 输出: [快照] name: Alice

  // db->ReleaseSnapshot(snapshot);  // 释放快照

  // ----------------- 遍历数据 -----------------
  leveldb::Iterator* it = db->NewIterator(read_options);
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }
  // 输出:
  // age: 25
  // city: New York
  // name: Bob
  delete it;  // 必须手动释放迭代器

  // ----------------- 删除数据 -----------------
  status = db->Delete(write_options, "city");
  if (status.ok()) {
    std::cout << "已删除键 'city'" << std::endl;
  }

  // ----------------- 关闭数据库 -----------------
  delete db;  // 关闭并释放资源
  return 0;
}