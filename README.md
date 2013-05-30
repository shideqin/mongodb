## About

`mongodb-lib` 基于[`node-mongodb-native`](https://mongodb.github.com/node-mongodb-native) 开发而来，提供一个简单、高可用的mongodb连接操作基础库。主要特性如下：

* 支持跨db连接操作；
* 支持连接池，基于collection的连接池控制；
* 支持初始化服务时，进行db的连接以降低处理时的连接成本；
* 支持连接auth认证；

## Install

```
直接下载
```

## Usage

```javascript

var mongo = require('mongodb-lib');

mongo.useDB(['db1','db2']);

//INSERT
mongo.insert("db1","collection1",{"id":1,"name":"one"},function(err,res){
	//回调处理
},options);


//UPDATE
mongo.update("db1","collection1",{$inc:{"id":2}},{"id":1},function(err,res){
	//回调处理
},options);

//GET ROW
mongo.get_row("db1","collection1",{"id":1},function(err,res){
	//回调处理
});

//GET RESULTS
mongo.get_results("db1","collection1",{"id":1},["id"],options,function(err,res){
	//回调处理
});

//GET COUNT
mongo.get_count("db1","collection1",{"id":1},function(err,res){
	//回调处理
});

//FIND AND MODIFY
mongo.findAndModify("db1","collection1",{$inc:{"id":2}},{"id":1},function(err,res){
	//回调处理
},options);


//REMOVE
mongo.remove("db1","collection1",{"id":1},function(err,res){
	//回调处理
},options);



```

## License

(The MIT License)

Copyright (c) 2012 shideqin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

