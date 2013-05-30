exports.connect = function(port,host,user,pass,server_options){
    if(typeof server_options == 'undefined' || server_options === null){
        server_options = {"auto_reconnect":true,"poolSize":5,"socketOptions":{"keepAlive":0}};
    }
    return new MongodbLib(port,host,user,pass,server_options);
};

/**
 * [MongodbLib Mongodb操作类]
 * @param {[type]} port           [端口]
 * @param {[type]} host           [主机]
 * @param {[type]} user           [用户]
 * @param {[type]} pass           [密码]
 * @param {[type]} server_options [选项参数]
 */
function MongodbLib(port,host,user,pass,server_options){

    var self = this;

    self._dbHandle = {};
    self._queryList = {};
    self._queryData = {};
    self._connStatus = {};
    self._useDb = null;

    /**
     * [useDB 初始化连接数据库]
     * @return {[type]} [无]
     */
    this.useDB = function(dbList){
        for(var db in dbList){
            self._authenticate(dbList[db]);
        }
    };

    /**
     * [get_row 获取单行结果]
     * @param  {[type]} dbName    [数据库名]
     * @param  {[type]} tableName [表名]
     * @param  {[type]} where     [条件]
     * @param  {[type]} backfun   [回调]
     * @return {[type]}           [mongodb单行结果json]
     */
    this.get_row = function(dbName,tableName,where,backfun){
        var options = {"row":true};
        self._fetch(dbName,tableName,where,null,options,backfun);
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
    this.get_results = function(dbName,tableName,where,fields,options,backfun){
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
        self._fetch(dbName,tableName,where,_fields,options,backfun);
    };

    /**
     * [get_count 获取结果统计数]
     * @param  {[type]} dbName    [数据库名]
     * @param  {[type]} tableName [表名]
     * @param  {[type]} where     [条件]
     * @param  {[type]} backfun   [回调]
     * @return {[type]}           [mongodb结果统计数int]
     */
    this.get_count = function(dbName,tableName,where,backfun){
        var options = {"count":true};
        self._fetch(dbName,tableName,where,null,options,backfun);
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
    this.insert = function(dbName,tableName,docs,backfun,options){
        if(typeof options == 'undefined' || options === null){
            options = {"safe":true};
        }
        if(typeof backfun == 'undefined' || backfun === null){
            backfun = function(){};
        }
        self._query(dbName,tableName,null,docs,options,function(err,collection){
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
    this.update = function(dbName,tableName,docs,where,backfun,options){
        if(typeof options == 'undefined' || options === null){
            options = {"safe":true};
        }
        if(typeof backfun == 'undefined' || backfun === null){
            backfun = function(){};
        }
        self._query(dbName,tableName,where,docs,options,function(err,collection){
            collection.update(self._queryData[dbName].where,self._queryData[dbName].fields,self._queryData[dbName].options,backfun);
        });
    };

    /**
     * [findAndModify 更新并返回]
     * @param  {[type]} dbName    [数据库名]
     * @param  {[type]} tableName [表名]
     * @param  {[type]} docs      [更新集合]
     * @param  {[type]} where     [条件]
     * @param  {[type]} backfun   [回调]
     * @param  {[type]} options   [选项参数]
     * @return {[type]}           [成功/失败]
     */
    this.findAndModify = function(dbName,tableName,docs,where,backfun,options){
        if(typeof options == 'undefined' || options === null){
            options = {"safe":true,"upsert":true,"new":true};
        }
        if(typeof backfun == 'undefined' || backfun === null){
            backfun = function(){};
        }
        self._query(dbName,tableName,where,docs,options,function(err,collection){
            collection.findAndModify(self._queryData[dbName].where,[],self._queryData[dbName].fields,self._queryData[dbName].options,backfun);
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
    this.remove = function(dbName,tableName,where,backfun,options){
        if(typeof options == 'undefined' || options === null){
            options = {"safe":true};
        }
        if(typeof backfun == 'undefined' || backfun === null){
            backfun = function(){};
        }
        self._query(dbName,tableName,where,null,options,function(err,collection){
            collection.remove(self._queryData[dbName].where,self._queryData[dbName].options,backfun);
        });
    };

    this._fetch = function(dbName,tableName,where,fields,options,backfun){
        self._query(dbName,tableName,where,fields,options,function(err,collection){
            if(err){
                console.error('collection failed',err);
            }
            if(self._queryData[dbName].options['row'] === true){
                collection.findOne(self._queryData[dbName].where,function(err,docs){
                    if(err){
                        self._close();
                    }
                    backfun(err,docs);
                });
            }
            else if(self._queryData[dbName].options['count'] === true){
                collection.find(self._queryData[dbName].where).count(function(err,docs){
                    if(err){
                        self._close();
                    }
                    backfun(err,docs);
                });
            }else{
                collection.find(self._queryData[dbName].where,self._queryData[dbName].fields,self._queryData[dbName].options).toArray(function(err,docs){
                    if(err){
                        self._close();
                    }
                    backfun(err,docs);
                });
            }
        });
    };

    this._query = function(dbName,tableName,where,fields,options,backfun){
        if(!self._queryList[dbName]){
            self._queryList[dbName] = [];
        }
        self._queryList[dbName].push({"tableName":tableName,"where":where,"fields":fields,"options":options,"backfun":backfun});

        if(!self._connStatus[dbName]){
            self._authenticate(dbName);
        }else{
            self._queryQueue();
        }
    };

    this._queryQueue = function(){
        for(var dbName in self._queryList){
            self._useDb = dbName;
            self._queueExec(dbName);
        }
    };

    this._queueExec = function(dbName){
        if(self._queryList[dbName].length > 0){
            if(typeof self._dbHandle[dbName] === 'object'){
                self._queryData[dbName] = self._queryList[dbName].shift();
                self._dbHandle[dbName].collection(self._queryData[dbName].tableName,self._queryData[dbName].backfun);
                self._queueExec(dbName);
            }
        }
    };

    this._authenticate = function(dbName){
        if(typeof self._dbHandle[dbName] !== 'object'){
            self._connStatus[dbName] = true;
            var Server = require('mongodb').Server(host,port,server_options);
            var Db = require('mongodb').Db;
            var Cursor = new Db(dbName,Server,{});
            Cursor.open(function(err,db){
                if(err){
                    console.error('connect failed',err);
                }else{
                    db.admin(function(err,adminDb){
                        adminDb.authenticate(user,pass,function(err,result){
                            if(err){
                                console.error(err);
                            }
                            self._dbHandle[dbName] = db;
                            self._queryQueue();
                        });
                    });
                }
            });
        }
    };

    this._close = function(){
        self._useDb.close();
        self._dbHandle = {};
        self._queryList = {};
        self._queryData = {};
        self._connStatus = {};
        self._useDb = null;
    };
}