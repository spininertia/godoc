#!/usr/bin/env node

var ot = require('ot');
var express = require('express');
var socketIO = require('socket.io');
var path = require('path');
var http = require('http');

var app = express();
var appServer = http.createServer(app);

app.configure(function () {
  app.use(express.logger());
  app.use('/', express.static(path.join(__dirname, '../../public')));
  app.use('/static', express.static(path.join(__dirname, '../../public')));
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
});

var io = socketIO.listen(appServer);

// source: http://devcenter.heroku.com/articles/using-socket-io-with-node-js-on-heroku
io.configure('production', function () {
  io.set('transports', ['xhr-polling']);
  io.set('polling duration', 10);
});

// Read config file
var configFile = process.argv[2]
var appServerList;
var otServerList;
var meOtAddr;
var meAppAddr;

var me = parseInt(process.argv[3]);
console.log('me is :' + me);

fs = require("fs")
fs.readFile(configFile, 'utf8', function(err, data){
    if (err) {
        console.log('error to read config file ' + configFile);
        return;
    } else {
        lines = data.split('\n');
        appServerList = lines[0].split(' ');
        otServerList = lines[1].split(' ');
        meOtAddr = otServerList[me];
        meAppAddr = appServerList[me];

        // show info
        console.log("app server list: " + appServerList);
        console.log('ot server list: ' + otServerList);
        console.log('me ot addr: ' + meOtAddr);
        console.log('me app addr: ' + meAppAddr);

        var port = meOtAddr.split(':')[1];
        appServer.listen(port, function () {
            console.log("Listening on port " + port);
        });

        process.on('uncaughtException', function (exc) {
            console.error(exc);
        });
    }
});

// whether the server has went to AppServer to get the lastest document
var docUpdated = false;
var str;
var docId = 'demo';
var socketIOServer;

var startSeq;

function updateList() {
  console.log('fetch new server list');
  parts = meAppAddr.split(':')
  var options = {
    hostname: parts[0],
    port: parseInt(parts[1]), // use configuration
    path: '/get_serverlist',
    method: 'GET'
  };

  var req = http.request(options, function (res) {
    res.setEncoding('utf8');
    res.on('data', function (chunk) {
      console.log("Receive server list : " + chunk);
      serverList = chunk.trim();
      otServerList = serverList.split(' ');
      meOtAddr = otServerList[me];
    });

    io.sockets.emit('update_serverlist', otServerList.join(' ') + ' ' + me);
  }).on("error", function (e) {
    console.log("Get document error " + e);
  });
  req.end();
}

setInterval(updateList, 10000);

function initDocument(cb, socket) {
  docUpdated = true;

  parts = meAppAddr.split(':')
  var options = {
    hostname: parts[0],
    port: parseInt(parts[1]), // use configuration
    path: '/get_doc?doc_id=' + docId,
    method: 'GET'
  };

  console.log('options is:' + options);

  var req = http.request(options, function (res) {
    res.setEncoding('utf8');
    res.on('data', function (chunk) {
      console.log("Receive response : " + chunk);
      value = JSON.parse(chunk);
      startSeq = value['seq'] + 1;
      socketIOServer = new ot.EditorSocketIOServer(value['doc'], startSeq, [], docId, function (socket, cb) {
        cb(!!socket.mayEdit);
      });

      cb(socket);
    });
  }).on("error", function (e) {
    console.log("Get document error " + e);
  });
  req.end();
}

function addClient(socket) {
    parts = meAppAddr.split(':');
    var options = {
        hostname: parts[0],
        port: parseInt(parts[1]), // use configuration
        path: '/new_instr',
        method: 'GET'
    };
  console.log('add client');
  socketIOServer.addClient(socket, options);
  socket.on('login', function (obj) {
    if (typeof obj.name !== 'string') {
      console.error('obj.name is not a string');
      return;
    }
    socket.mayEdit = true;
    socketIOServer.setName(socket, obj.name);
    socket.emit('logged_in', {});
    socket.emit('update_serverlist', otServerList.join(' ') + ' ' + me);
  });
}

io.sockets.on('connection', function (socket) {
  if (!docUpdated) {
    // update lastest document
    console.log("Init: get lastest document");
    initDocument(addClient, socket);
  } else {
    addClient(socket);
  }
});
