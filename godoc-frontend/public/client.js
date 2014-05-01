(function () {
  var EditorClient = ot.EditorClient;
  var SocketIOAdapter = ot.SocketIOAdapter;
  var AjaxAdapter = ot.AjaxAdapter;
  var CodeMirrorAdapter = ot.CodeMirrorAdapter;

  var socket;

  // uncomment to simulate more latency
  /*(function () {
    var emit = socket.emit;
    var queue = [];
    socket.emit = function () {
      queue.push(arguments);
      return socket;
    };
    setInterval(function () {
      if (queue.length) {
        emit.apply(socket, queue.shift());
      }
    }, 800);
  })();*/

  var disabledRegex = /(^|\s+)disabled($|\s+)/;
  var otServerList;
  var meIndex;
  var username;

  function updateServerList(op) {
    console.log('op is : ' + op);
    var parts = op.split(' ');
    meIndex = parseInt(parts[parts.length - 1]);
    otServerList = parts.slice(0, parts.length-1);
    console.log('receive update server list ' + otServerList);
    console.log('me index is:' + meIndex);
  } 

  var login;
  if (useSocketIO) {
    login = function (username, callback) {
      console.log('login user: ' + username);
      socket
        .emit('login', { name: username })
        .on('logged_in', callback)
        .on('update_serverlist', updateServerList);
    };
  } else {
    login = function (username, callback) {
      $.ajax({
        method: 'GET',
        url: '/login/' + encodeURIComponent(username),
        success: function () { callback(); },
        error: function () {
          alert("Login failed!");
        }
      });
    };
  }

  function enable (el) {
    el.className = el.className.replace(disabledRegex, ' ');
  }

  function disable (el) {
    if (!disabledRegex.test(el.className)) {
      el.className += ' disabled';
    }
  }

  function preventDefault (e) {
    if (e.preventDefault) { e.preventDefault(); }
  }

  function stopPropagation (e) {
    if (e.stopPropagation) { e.stopPropagation(); }
  }

  function stopEvent (e) {
    preventDefault(e);
    stopPropagation(e);
  }

  function removeElement (el) {
    el.parentNode.removeChild(el);
  }

  function beginsWith (a, b) { return a.slice(0, b.length) === b; }
  function endsWith (a, b) { return a.slice(a.length - b.length, a.length) === b; }

  function wrap (chars) {
    cm.operation(function () {
      if (cm.somethingSelected()) {
        cm.replaceSelections(cm.getSelections().map(function (selection) {
          if (beginsWith(selection, chars) && endsWith(selection, chars)) {
            return selection.slice(chars.length, selection.length - chars.length);
          }
          return chars + selection + chars;
        }), 'around');
      } else {
        var index = cm.indexFromPos(cm.getCursor());
        cm.replaceSelection(chars + chars);
        cm.setCursor(cm.posFromIndex(index + 2));
      }
    });
    cm.focus();
  }

  function bold ()   { wrap('**'); }
  function italic () { wrap('*'); }
  function code ()   { wrap('`'); }

  var editorWrapper = document.getElementById('editor-wrapper');
  var cm = window.cm = CodeMirror(editorWrapper, {
    lineNumbers: true,
    lineWrapping: true,
    mode: 'markdown',
    readOnly: 'nocursor'
  });

  var undoBtn = document.getElementById('undo-btn');
  undoBtn.onclick = function (e) { cm.undo(); cm.focus(); stopEvent(e); };
  disable(undoBtn);
  var redoBtn = document.getElementById('redo-btn');
  redoBtn.onclick = function (e) { cm.redo(); cm.focus(); stopEvent(e); };
  disable(redoBtn);

  var boldBtn = document.getElementById('bold-btn');
  boldBtn.onclick = function (e) { bold(); stopEvent(e); };
  disable(boldBtn);
  var italicBtn = document.getElementById('italic-btn');
  italicBtn.onclick = function (e) { italic(); stopEvent(e); };
  disable(italicBtn);
  var codeBtn = document.getElementById('code-btn');
  disable(codeBtn);
  codeBtn.onclick = function (e) { code(); stopEvent(e); };

  var loginForm = document.getElementById('login-form');
  loginForm.onsubmit = function (e) {
    preventDefault(e);
    username = document.getElementById('username').value;
    login(username, function () {
      var li = document.createElement('li');
      li.appendChild(document.createTextNode(username + " (that's you!)"));
      cmClient.clientListEl.appendChild(li);
      cmClient.serverAdapter.ownUserName = username;

      enable(boldBtn);
      enable(italicBtn);
      enable(codeBtn);

      cm.setOption('readOnly', false);
      removeElement(overlay);
      removeElement(loginForm);
    });
  };

  var overlay = document.createElement('div');
  overlay.id = 'overlay';
  overlay.onclick = stopPropagation;
  overlay.onmousedown = stopPropagation;
  overlay.onmouseup = stopPropagation;
  var cmWrapper = cm.getWrapperElement();
  cmWrapper.appendChild(overlay);

  /**
   * Reconnect to operation server
   */
  function reconnect() {
    console.log('disconnect old connection first');
    if (!!socket) {
      console.log('socket is ' + socket);
      socket.disconnect();
    }
    
    meIndex = meIndex + 1;
    meIndex = meIndex % (otServerList.length);
    console.log('reconnect to different server:' + otServerList[meIndex]);
    alert('reconnect to different server:' + otServerList[meIndex]);
    
    socket = io.connect('http://' + otServerList[meIndex], {'reconnec':false});
    console.log('reconnec cmd sent');
    socket.on('doc', function (obj) {
      console.log('get document');
      init(obj.str, obj.revision, obj.clients, new SocketIOAdapter(socket));

      login(username, function () {
        cmClient.serverAdapter.ownUserName = username;

        enable(boldBtn);
        enable(italicBtn);
        enable(codeBtn);
      });
    })
    .on('disconnect', function (obj) {
      console.log('server crashed');
      reconnect();
    })
    .on('update_serverlist', updateServerList);

    
  }

  var cmClient;
  if (useSocketIO) {
    socket = io.connect('/', {'reconnect' : false});
    socket.on('doc', function (obj) {
      init(obj.str, obj.revision, obj.clients, new SocketIOAdapter(socket));
    })
    .on('disconnect', function (obj) {
      console.log('server crashed');
      reconnect();
    });
  } else {
    $.ajax({
      method: 'GET',
      url: '/ot',
      dataType: 'json',
      success: function (obj) {
        var users = {};
        for (var name in obj.users) {
          if (obj.users.hasOwnProperty(name)) {
            users[name] = { name: name, cursor: obj.users[name] };
          }
        }
        init(obj.document, obj.revision.major, users, new AjaxAdapter('/ot', {}, obj.revision));
      },
      error: function () {
        alert("Failed to load document state!");
      }
    });
  }

  function init (str, revision, clients, serverAdapter) {
    cm.setValue(str);
    cmClient = window.cmClient = new EditorClient(
      revision, clients,
      serverAdapter, new CodeMirrorAdapter(cm)
    );

    var userListWrapper = document.getElementById('userlist-wrapper');
    userListWrapper.appendChild(cmClient.clientListEl);
    
    cm.on('change', function () {
      if (!cmClient) { return; }
      console.log(cmClient.undoManager.canUndo(), cmClient.undoManager.canRedo());
      (cmClient.undoManager.canUndo() ? enable : disable)(undoBtn);
      (cmClient.undoManager.canRedo() ? enable : disable)(redoBtn);
    });
  }
})();