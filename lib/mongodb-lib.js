var mongodb = require('mongodb');

exports.connect = function(host,user,pass,options){
    if(typeof options == 'undefined' || options === null){
        options = {
            poolSize:10,
            reconnectTries:1200,
            reconnectInterval:30000,
            socketOptions:{
                keepAlive:5000
            }
        };
    }
    return new MongodbLib(host,user,pass,options);
};

/**
 * see api http://mongodb.github.io/node-mongodb-native/2.2/api/
 * [MongodbLib Mongodb操作类]
 * @param {[type]} host    [主机]
 * @param {[type]} user    [用户]
 * @param {[type]} pass    [密码]
 * @param {[type]} options [选项参数]
 */
function MongodbLib(host,user,pass,options){
    this._dbHandle = {};
    this._queryList = {};
    this._queryData = {};
    this._connStatus = {};
    
    this.host = host;
    this.user = user;
    this.pass = pass;
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
    return mongodb.ObjectID(_id);
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
    var options = {"row":true};
    this._fetch(dbName,tableName,where,null,options,backfun);
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
    var _fields = {};
    if(typeof options == 'undefined' || options === null){
        options = {};
    }
    if(fields instanceof Array){
        for(var _key = 0; _key < fields.length; _key++){
            _fields[fields[_key]] = fields[_key];
        }
    }else{
        _fields = fields;
    }
    this._fetch(dbName,tableName,where,_fields,options,backfun);
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
    var options = {"count":true};
    this._fetch(dbName,tableName,where,null,options,backfun);
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
    var self = this;
    if(typeof options == 'undefined' || options === null){
        options = {"safe":true};
    }
    if(typeof backfun == 'undefined' || backfun === null){
        backfun = function(){};
    }
    this._query(dbName,tableName,null,docs,options,function(err,collection){
        collection.insert(self._queryData[dbName].fields,self._queryData[dbName].options,backfun);
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
    var self = this;
    if(typeof options == 'undefined' || options === null){
        options = {"safe":true};
    }
    if(typeof backfun == 'undefined' || backfun === null){
        backfun = function(){};
    }
    this._query(dbName,tableName,where,docs,options,function(err,collection){
        collection.update(self._queryData[dbName].where,self._queryData[dbName].fields,self._queryData[dbName].options,backfun);
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
    var self = this;
    if(typeof options == 'undefined' || options === null){
        options = {"safe":true,"upsert":true,"new":true};
    }
    if(typeof backfun == 'undefined' || backfun === null){
        backfun = function(){};
    }
    this._query(dbName,tableName,where,docs,options,function(err,collection){
        collection.findAndModify(self._queryData[dbName].where,[],self._queryData[dbName].fields,self._queryData[dbName].options,backfun);
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
    var self = this;
    if(typeof options == 'undefined' || options === null){
        options = {};
    }
    if(typeof backfun == 'undefined' || backfun === null){
        backfun = function(){};
    }
    this._query(dbName,tableName,where,null,options,function(err,collection){
        collection.findAndRemove(self._queryData[dbName].where,[],self._queryData[dbName].options,backfun);
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
    var self = this;
    if(typeof options == 'undefined' || options === null){
        options = {"safe":true};
    }
    if(typeof backfun == 'undefined' || backfun === null){
        backfun = function(){};
    }
    this._query(dbName,tableName,where,null,options,function(err,collection){
        collection.remove(self._queryData[dbName].where,self._queryData[dbName].options,backfun);
    });
};

//find方法支持自定义协议
MongodbLib.prototype._fetch = function(dbName,tableName,where,fields,options,backfun){
    var start = new Date().getTime();
    var self = this;
    self._query(dbName,tableName,where,fields,options,function(err,collection){
        if(err){
            console.error('collection failed',err);
        }
        if(self._queryData[dbName].options['row'] === true){
            collection.findOne(self._queryData[dbName].where,backfun);
        }
        else if(self._queryData[dbName].options['count'] === true){
            collection.find(self._queryData[dbName].where).count(backfun);
        }else{
            collection.find(self._queryData[dbName].where,self._queryData[dbName].fields,self._queryData[dbName].options).toArray(backfun);
        }
    });
};

//入query队列
MongodbLib.prototype._query = function(dbName,tableName,where,fields,options,backfun){
    if(!this._queryList[dbName]){
        this._queryList[dbName] = [];
    }
    this._queryList[dbName].push({"tableName":tableName,"where":where,"fields":fields,"options":options,"backfun":backfun});

    if(!this._connStatus[dbName]){
        this._authenticate(dbName);
    }else{
        this._queryQueue();
    }
};

//取出query队列
MongodbLib.prototype._queryQueue = function(){
    for(var dbName in this._queryList){
        this._queueExec(dbName);
    }
};

//执行query队列
MongodbLib.prototype._queueExec = function(dbName){
    if(this._queryList[dbName].length > 0){
        if(typeof this._dbHandle[dbName] === 'object'){
            this._queryData[dbName] = this._queryList[dbName].shift();
            if(this._queryData[dbName].tableName != null){
                this._dbHandle[dbName].collection(this._queryData[dbName].tableName,this._queryData[dbName].backfun);
                this._queueExec(dbName);
            }
        }
    }
};

//获取db连接及验证
MongodbLib.prototype._authenticate = function(dbName){
    var self = this;
    if(typeof self._dbHandle[dbName] !== 'object'){
        self._connStatus[dbName] = true;
        var mongoServers = self._mongoServers(mongodb.Server);
        var mongoConn = mongodb.Db(dbName,mongoServers.server,mongoServers.options);
        mongoConn.open(function(err,db){
            if(err){
                console.error('connect failed',err);
                return;
            }
            if(!self.user || !self.pass){
                self._dbHandle[dbName] = db;
                self._queryQueue();
                return;
            }
            db.admin(function(err,adminDb){
                adminDb.authenticate(self.user,self.pass,function(err,result){
                    if(err){
                        console.error(err);
                        return;
                    }
                    self._dbHandle[dbName] = db;
                    self._queryQueue();
                });
            });
        });
    }
};

//副本集集群
MongodbLib.prototype._mongoServers = function(Server){
    var self = this;
    var hosts = self.host.split(',');
    var hostCount = hosts.length;
    var mongoOptions = {};
    if(hostCount < 2){
        var tmp = hosts[0].split(':');
        return {server:Server(tmp[0],Number(tmp[1]),self.options),options:mongoOptions};
    }
    var replSetServers = [];
    for(var i in hosts){
        var tmp = hosts[i].split(':');
        replSetServers.push(Server(tmp[0],Number(tmp[1]),self.options));
    }
    var ReplSet = mongodb.ReplSet;
    var ReadPreference = mongodb.ReadPreference;
    var replStat = new ReplSet(replSetServers,{
        haInterval:1000,
        reconnectWait:1000
    });
    mongoOptions = {
        readPreference:ReadPreference.PRIMARY_PREFERRED
    }
    return {server:replStat,options:mongoOptions};
};