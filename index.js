// Load the AWS SDK for Node.js
let AWS = require('aws-sdk');

module.exports = function(region) {
  // Set the region
  AWS.config.update({region: region});

  let ENABLE_LOGGING = false;
  let log = function() {
    if (ENABLE_LOGGING) { console.log.apply(console,arguments); }
  };

  // Create DynamoDB document client
  let ddb = new AWS.DynamoDB();
  let docClient = new AWS.DynamoDB.DocumentClient();

  let wrapper = {
    'document':docClient
    ,'ddb':ddb

    ,'enable_logging':function() { ENABLE_LOGGING=true; }
    ,'disable_logging':function() { ENABLE_LOGGING=false; }
    ,'logging':function(bool) { ENABLE_LOGGING=bool; }
    ,'lock_table':'LOCK'
    ,'delay': async function(duration) { return new Promise(resolve => setTimeout(resolve, duration)); }

    // DELETE A TABLE
    // ==============
    ,'deleteTable': async function(tableName) {
      // Call DynamoDB to delete the specified table
      return ddb.deleteTable({"TableName":tableName}).promise();
    }

    // CREATE A TABLE
    // ==============
    ,'createSimpleTable': async function(tableName, key, keyType) {
      let params = {
        AttributeDefinitions: [
          {
            AttributeName: key,
            AttributeType: (keyType || "S")
          }
        ],
        KeySchema: [
          {
            AttributeName: key,
            KeyType: 'HASH'
          }
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: 5,
          WriteCapacityUnits: 5
        },
        TableName: tableName,
        StreamSpecification: {
          StreamEnabled: false
        }
      };

      // Call DynamoDB to create the table
      return ddb.createTable(params).promise();
    }

    ,'describe': async function(table) {
      return ddb.describeTable( {TableName:table} ).promise();
    }

    // WAIT FOR A TABLE TO EXIST BEFORE PROCEEDING
    // ===========================================
    // TODO: Use waitFor() from the API instead
    ,'when_table_exists': async function(table,wait,max_retries) {
      return new Promise(function(resolve,reject) {
        let max_retries = max_retries || 50;
        let wait = wait || 1000;
        let check = function() {
          wrapper.describe(table)
            .then( (data)=>{
              if (data && data.Table && "ACTIVE"==data.Table.TableStatus) {
                return resolve(data);
              }
              throw("Not Active");
            })
            .catch( (err)=>{
              if (max_retries-- > 0) {
                setTimeout(check,wait);
              }
              else {
                reject(err);
              }
            })
        };
        check();
      });
    }

    // PSEUDO-LOCK ON AN ARBITRARY KEY (OR TABLE NAME)
    // ===============================================
    ,'lock': async function(lock_key, retry_count, retry_delay_ms, func) {
      log("Trying to get lock");
      retry_count = retry_count || 25;
      retry_delay_ms = retry_delay_ms || 50;
      let attempt_put = async function() {
        try {
          let now = (new Date()).getTime();
          await wrapper.putUnique(wrapper.lock_table, {"key":lock_key, "time":now}, "key" );
          return true;
        }
        catch(e) {
          //console.log("Put fail");
          return false;
        }
      };
      let release = async function() {
        console.log("Releasing lock");
        return wrapper.delete(wrapper.lock_table, "key", lock_key);
      };
      while (retry_count-- >= 0) {
        console.log("retry_count: "+retry_count);
        let result = await attempt_put();
        if (result) {
          try {
            await func();
            await release();
            return true;
          }
          catch(e) {
            await release();
            throw e;
          }
        }
        await wrapper.delay(retry_delay_ms);
      }
      throw "lock_timeout";
    }

    // INSERT
    // ======
    ,'put': async function(table, item) {
      let params = {TableName:table, Item:item};
      log(params);
      return docClient.put( params ).promise();
    }
    ,'putUnique': async function(table, item, key_attribute) {
      let params = {TableName:table, Item:item, ConditionExpression:'attribute_not_exists(#keyAttribute)', ExpressionAttributeNames:{"#keyAttribute":key_attribute} };
      log(params);
      return docClient.put( params ).promise();
    }

    // DELETE
    // ======
    ,'delete': async function(table, keyAttribute, keyValue) {
      let params = {TableName:table, Key:{ [keyAttribute]:keyValue }};
      log(params);
      return docClient.delete(params).promise();
    }

    // UPDATE
    // ======
    ,'update': async function(table, keyAttribute, data, condition) {
      let i=0, k, name, val;
      let update_expression = [];
      let names={}, values = {};
      let params;
      let extract_names = function(name) {
        let parts=[];
        if (typeof name==="string") {
          for (let part of name.split('.')) {
            let k = "#" + i++;
            let non_array_part = part.replace(/\[[^\]]*\]/g,'');
            parts.push(part.replace(non_array_part,k));
            names[k] = non_array_part;
          }
          return parts.join(".");
        }
        return "";
      };
      if (typeof table==="string") {
          let keynum = 0;
        for (k in data) {
          if (k === keyAttribute) {
              keynum=i;
            names["#" + i++] = k;
          }
          else {
            val = data[k];
            if (/^\+/.test(k)) {
              // increment a value
              if (typeof val === "number") {
                k = extract_names(k.substring(1));
                update_expression.push(` ${k} = ${k} + :${i} `);
              }
            }
            else if (/^list_append /.test(k)) {
              // append to list
              k = extract_names(k.substring(12));
              update_expression.push(` ${k} = list_append(${k},:${i}) `);
            }
            else {
              k = extract_names(k);
              update_expression.push(` ${k} = :${i} `);
            }
            values[":" + i] = val;
            i++;
          }
        }
        update_expression = update_expression.join(",");
        params = {
          TableName: table
          , Key: {}
          , UpdateExpression: 'set ' + update_expression
          , ExpressionAttributeNames: names
          , ExpressionAttributeValues: values
          , ConditionExpression: "attribute_exists(#"+keynum+")"
        };
        if (condition && typeof condition === "string") {
          condition = condition.replace(/\{\{([^\}]+)\}\}/g, function (m, mm) {
            let val = mm;
            let key = ":" + (i++);
            if (+val == val) {
              val = +val;
            }
            values[key] = val;
            return key;
          });
          params.ConditionExpression += " AND " + condition;
        }
        params.Key[keyAttribute] = data[keyAttribute];
      }
      else {
        params = table;
      }

      log(params);
      return docClient.update(params).promise();
    }

    // RETRIEVE
    // ========
    ,'get': async function(table, keyAttribute, keyValue) {
      let params = {TableName:table, Key:{ [keyAttribute]:keyValue } };
      log(params);
      return docClient.get(params).promise()
        .then( (item)=> {
          if (!item || !item.Item) { return null; }
          return item.Item;
        });
    }
    ,'scan': async function(table, params) {
      params = params || {};
      params.TableName = table;
      log(params);
      let lastKey = null;
      let results = [];
      return new Promise(async (resolve,reject)=>{
        do {
          if (lastKey) { params.ExclusiveStartKey = lastKey; }
          await docClient.scan(params).promise()
            .then( (items)=> {
              if (!items || !items.Items || !items.Items.length) {
                lastKey=null;
              }
              else {
                results = results.concat(items.Items);
                lastKey = items.LastEvaluatedKey;
              }
            }).catch( (err)=>{ reject(err); } );
        } while (lastKey);
        resolve(results);
      });
    }
  };

  return wrapper;
};
