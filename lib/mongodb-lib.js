var MongoClient = require('mongodb').MongoClient;
var ObjectID = require('mongodb').ObjectID;
exports.connect = function(host,user,passwd,options){
    if(!options){
        options = {};
    }
    if(!options['poolSize']){
        options['poolSize'] = 1;
    }
    if(!options['reconnectTries']){
        options['reconnectTries'] = 86400;
    }
    if(!options['reconnectInterval']){
        options['reconnectInterval'] = 5000;
    }
    if(!options['keepAlive']){
        options['keepAlive'] = 5000;
    }
    return new MongodbLib(host,user,passwd,options);
};

/**
 * see api http://mongodb.github.io/node-mongodb-native/2.2/api/
 * [MongodbLib Mongodb操作类]
 * @param {[type]} host    [主机]
 * @param {[type]} user    [用户]
 * @param {[type]} passwd  [密码]
 * @param {[type]} options [选项参数]
 */
function MongodbLib(host,user,passwd,options){
    this._dbHandle = {};
    
    this.host = host;
    this.user = user;
    this.passwd = passwd;
    this.options = options;
}

/**
* [useDB 初始化连接数据库]
* @return {[type]} [无]
*/
MongodbLib.prototype.useDB = function(dbList){
    for(var db in dbList){
        this._authenticate(dbList[db]);
    }
};

/**
* [ObjectID 将字符_id转成对象_id]
* @return {[type]} [无]
*/

MongodbLib.prototype.ObjectID = function(_id){
    return new ObjectID(_id);
};

/**
 * [get_row 获取单行结果]
 * @param  {[type]} dbName    [数据库名]
 * @param  {[type]} tableName [表名]
 * @param  {[type]} where     [条件]
 * @param  {[type]} backfun   [回调]
 * @return {[type]}           [mongodb单行结果json]
 */
MongodbLib.prototype.get_row = function(dbName,tableName,where,backfun){
    if(!backfun){
        console.log('get_row::backfun is undefined');
        return;
    }
    if(!where){
        where = {};
    }
    var options = {"row":true};
    this._fetch(dbName,tableName,where,options,backfun);
};

/**
 * [get_results 获取一个结果集]
 * @param  {[type]} dbName    [数据库名]
 * @param  {[type]} tableName [表名]
 * @param  {[type]} where     [条件]
 * @param  {[type]} fields    [需要显示的字段]
 * @param  {[type]} options   [选项参数]
 * @param  {[type]} backfun   [回调]
 * @return {[type]}           [mongodb结果集json]
 */
MongodbLib.prototype.get_results = function(dbName,tableName,where,fields,options,backfun){
    if(!backfun){
        console.log('get_results::backfun is undefined');
        return;
    }
    if(!where){
        where = {};
    }
    if(!fields){
        fields = {};
    }
    if(!options){
        options = {};
    }
    var _fields = {};
    if(fields instanceof Array){
        for(var _key in fields){
            _fields[fields[_key]] = 1;
        }
    }else{
        _fields = fields;
    }
    options['fields'] = _fields;
    this._fetch(dbName,tableName,where,options,backfun);
};

/**
 * [get_count 获取结果统计数]
 * @param  {[type]} dbName    [数据库名]
 * @param  {[type]} tableName [表名]
 * @param  {[type]} where     [条件]
 * @param  {[type]} backfun   [回调]
 * @return {[type]}           [mongodb结果统计数int]
 */
MongodbLib.prototype.get_count = function(dbName,tableName,where,backfun){
    if(!backfun){
        console.log('get_count::backfun is undefined');
        return;
    }
    if(!where){
        where = {};
    }
    var options = {"count":true};
    this._fetch(dbName,tableName,where,options,backfun);
};

/**
 * [insert 插入]
 * @param  {[type]} dbName    [数据库名]
 * @param  {[type]} tableName [表名]
 * @param  {[type]} docs      [插入集合]
 * @param  {[type]} backfun   [回调]
 * @param  {[type]} options   [选项参数]
 * @return {[type]}           [成功/失败]
 */
MongodbLib.prototype.insert = function(dbName,tableName,docs,backfun,options){
    if(!backfun){
        backfun = function(){};
    }
    if(!options){
        options = {w:1};
    }
    this._query(dbName,tableName,function(err,collection){
        if(err){
            backfun(err,null);
            return;
        }
        collection.insert(docs,options,backfun);
    });
};

/**
 * [update 更新]
 * @param  {[type]} dbName    [数据库名]
 * @param  {[type]} tableName [表名]
 * @param  {[type]} docs      [更新集合]
 * @param  {[type]} where     [条件]
 * @param  {[type]} backfun   [回调]
 * @param  {[type]} options   [选项参数]
 * @return {[type]}           [成功/失败]
 */
MongodbLib.prototype.update = function(dbName,tableName,docs,where,backfun,options){
    if(!backfun){
        backfun = function(){};
    }
    if(!options){
        options = {w:1};
    }
    this._query(dbName,tableName,function(err,collection){
        if(err){
            backfun(err,null);
            return;
        }
        collection.update(where,docs,options,function(err,result){
            if(err){
                backfun(err,null);
                return;
            }
            backfun(null,result.result);
        });
    });
};

/**
 * [findAndModify 查找并修改]
 * @param  {[type]} dbName    [数据库名]
 * @param  {[type]} tableName [表名]
 * @param  {[type]} docs      [更新集合]
 * @param  {[type]} where     [条件]
 * @param  {[type]} backfun   [回调]
 * @param  {[type]} options   [选项参数]
 * @return {[type]}           [成功/失败]
 */
MongodbLib.prototype.findAndModify = function(dbName,tableName,docs,where,backfun,options){
    if(!backfun){
        backfun = function(){};
    }
    if(!options){
        options = {w:1,upsert:true,new:true};
    }
    this._query(dbName,tableName,function(err,collection){
        if(err){
            backfun(err,null);
            return;
        }
        collection.findAndModify(where,[],docs,options,backfun);
    });
};

/**
 * [findAndRemove 查找并删除]
 * @param  {[type]} dbName    [数据库名]
 * @param  {[type]} tableName [表名]
 * @param  {[type]} where     [条件]
 * @param  {[type]} backfun   [回调]
 * @param  {[type]} options   [选项参数]
 * @return {[type]}           [成功/失败]
 */
MongodbLib.prototype.findAndRemove = function(dbName,tableName,where,backfun,options){
    if(!backfun){
        backfun = function(){};
    }
    if(!options){
        options = {w:1};
    }
    this._query(dbName,tableName,function(err,collection){
        if(err){
            backfun(err,null);
            return;
        }
        collection.findAndRemove(where,[],options,backfun);
    });
};

/**
 * [remove 删除]
 * @param  {[type]} dbName    [数据库名]
 * @param  {[type]} tableName [表名]
 * @param  {[type]} where     [条件]
 * @param  {[type]} backfun   [回调]
 * @param  {[type]} options   [选项参数]
 * @return {[type]}           [成功/失败]
 */
MongodbLib.prototype.remove = function(dbName,tableName,where,backfun,options){
    if(!backfun){
        backfun = function(){};
    }
    if(!options){
        options = {w:1};
    }
    this._query(dbName,tableName,function(err,collection){
        if(err){
            backfun(err,null);
            return;
        }
        collection.deleteMany(where,options,function(err,result){
            if(err){
                backfun(err,null);
                return;
            }
            backfun(null,result.result);
        });
    });
};

//ping
MongodbLib.prototype.ping = function(backfun){
    this._authenticate('test',function(err){
        backfun(err);
    });
};

//find方法支持自定义协议
MongodbLib.prototype._fetch = function(dbName,tableName,where,options,backfun){
    this._query(dbName,tableName,function(err,collection){
        if(err){
            backfun(err,null);
            return;
        }
        if(options['row'] === true){
            collection.findOne(where,backfun);
            return;
        }
        if(options['count'] === true){
            collection.find(where).count(backfun);
            return;
        }
        collection.find(where,options).toArray(backfun);
    });
};

//入query队列
MongodbLib.prototype._query = function(dbName,tableName,backfun){
    this._authenticate(dbName,function(err,db){
        if(err){
            backfun(err,null);
            return;
        }
        db.collection(tableName,backfun);
    });
};

//获取db连接及验证
MongodbLib.prototype._authenticate = function(dbName,backfun){
    if(!backfun){
        backfun = function(){};
    }
    if(this._dbHandle[dbName]){
        backfun(null,this._dbHandle[dbName]);
        return;
    }
    var self = this;
    MongoClient.connect(this._url(),this.options,function(err,db){
        if(err){
            backfun(err,null);
            return;
        }
        var dataDbName = db.db(dbName);
        self._dbHandle[dbName] = dataDbName;
        backfun(null,dataDbName);
    });
};

//获取db的连接url
MongodbLib.prototype._url = function(){
    var url = 'mongodb://';
    var authDbName = '';
    if(this.user && this.passwd){
        url += this.user+':'+this.passwd+'@';
        authDbName = '/admin';
    }
    return url+this.host+authDbName;
};