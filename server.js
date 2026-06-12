// Render будет показывать полную ошибку в логах, а не просто завершать процесс.
process.on('uncaughtException', (err) => {
  console.error('UNCAUGHT ERROR:', err);
});

process.on('unhandledRejection', (err) => {
  console.error('UNHANDLED REJECTION:', err);
});


const { WebSocketServer } = require('ws');
const sqlite3 = require('sqlite3');
const host = '0.0.0.0';
const port = process.env.PORT || 3000;

const db = new sqlite3.Database('appDatabase.db', (err) => {
  if (err) {
    console.error('Ошибка подключения DB:', err.message);
    return;
  }
  console.log('База данных подключена');
});

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS registration (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      password TEXT NOT NULL
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      author TEXT NOT NULL,
      text TEXT NOT NULL,
      created_at TEXT NOT NULL
    )
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS friends (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      friend_name TEXT NOT NULL,
      text TEXT NOT NULL
      created_at TEXT NOT NULL
     )
    `
    
  )
});

const wss = new WebSocketServer({ host, port });
const clients = new Set();
const clientsId = new Map(); 
function broadcast(data) {
  const payload = JSON.stringify(data);
  for (const client of clients) {
    if (client.readyState === 1) {
      client.send(payload);
    }
  }
}

wss.on('listening', () => {
  console.log(`WebSocket создан: ws://${host}:${port}`);
});

wss.on('connection', (ws, req) => {
  clients.add(ws);

  const clientIp = req.socket.remoteAddress?.replace('::ffff:', '') || 'unknown';
  console.log(`Клиент подключился: ${clientIp}`);

  db.all(`SELECT id, author, text, created_at FROM messages ORDER BY id ASC`, [], (err, rows) => {
    if (err) {
      ws.send(JSON.stringify({
        type: 'error',
        message: 'Ошибка чтения истории из БД'
      }));
      return;
    }

    /*ws.send(JSON.stringify({
      type: 'history',
      message: rows
    }));*/
  });

  ws.on('message', (raw) => {
    try {
      const data = JSON.parse(raw.toString());

      if (data.type === 'friend_message') {
        const author = String(data.author || '').trim();
        const text = String(data.text || '').trim();

        if (!author || !text) return;

        const createdAt = new Date().toISOString();
        db.run(`INSERT INTO friends text VALUES ?`, [text], (err) => {
          if (err) {
              ws.send(JSON.stringify({
                type: 'error',
                message: 'Ошибка записи в БД'
              }));
              return;
            }
          
        });
        db.get(`SELECT friend_name FROM friends WHERE name = ?`, [author], (err, row) =>{
          const friend = row.friend_name;
        
          db.all(`SELECT text FROM friends WHERE name = ? AND friend_name = ?`, [author, friend], (err, rows) => {
            if (err) {
              ws.send(JSON.stringify({
                type: 'error',
                message: 'Ошибка БД'
              }));
              return;
            }
            ws.send(JSON.stringify({
              type: 'friend_message',
              text: rows
            }));
          });
        });
            broadcast({
              type: 'message',
              message: {
              id: this.lastID,
              author,
              text,
              created_at: createdAt
              }
            });
        
          
        
      }
///////////// получение данных логина
      if(data.type === 'login'){
        const name = String(data.name || '').trim();
        const password = String(data.password || '').trim();

        if(!name || !password) return;
       
        db.get(`SELECT id, name, password FROM registration WHERE name = ?`, [name], (err, row) => {

          if (err) {
            ws.send(JSON.stringify({
              type: 'error',
              message: 'Ошибка базы данных'
            }));
            return;
          }

          if (row && row.password === password) {
            ws.send(JSON.stringify({
              type: 'login_result',
              success: true,
              message: 'Вход выполнен успешно',
              user: {
                id: row.id,
                name: row.name
              }
            }));
            clientsId.set(row.id, ws); 
          } else {
              ws.send(JSON.stringify({
              type: 'login_result',
              success: false,
              message: 'Неверный логин или пароль'
              }));
            }
        });
      }
///////////// получение данных регистрации
      if(data.type === 'registration'){
        const nameregistr = String(data.nameregistr || '').trim();
        const passwordregistr = String(data.passwordregistr || '').trim();

        if(!nameregistr || !passwordregistr) return;
        
        db.run(`INSERT INTO registration (name, password) VALUES (?, ?)`, [nameregistr, passwordregistr], (err) => {
            if (err) {
              ws.send(JSON.stringify({
                type: 'error',
                message: 'Ошибка записи в БД'
              }));
              return;
            }
        });
        ws.send(JSON.stringify({
          type: 'register_result',
          message: 'Регистрация прошла успешно'
        }));
        db.get(`SELECT id, name FROM registration WHERE name = ?`, [nameregistr], (err, row) => {
          clientsId.set(row.id, ws);
        });
      }
      //////////// добавление в друзья
      if(data.type === 'name_friend'){
        const nameAdd = String(data.nameAdd || '').trim();
        //const nameSend = String(data.nameSend || '').trim();
        let send_invite_name = data.send_invite_name;
        if(!nameAdd) return;
        
        db.get(`SELECT id, name FROM registration WHERE name = ?`, [nameAdd], (err, row) => {
          if (err) {
            ws.send(JSON.stringify({
              type: 'error',
              message: 'Ошибка БД'
            }));
            return;
          }
          const friendSocket = clientsId.get(row.id);
          if(!friendSocket){
            ws.send(JSON.stringify({
              type: 'name_friend_result',
              success: false,
              message: 'Пользователь сейчас не в сети'
            }));
            return;
          }
          ws.send(JSON.stringify({
            type: 'name_friend_result',
            success: true,
            message: `Запрос в друзья пользователю ${nameAdd} отправлен`
          }));
          friendSocket.send(JSON.stringify({
            type: 'friend_request',
            message: `Запрос в друзья от ${send_invite_name}`,
            send_invite_name,
            sender: send_invite_name
          }));
        });
      }
///////////// получение имени для добавления в друзья
      if(data.type === 'friend_accept'){
        const sender = String(data.sender || '').trim();
        const namefriend = String(data.currentUserName || '').trim();
        let get_invite_name = data.currentUserName;
        db.get(`SELECT id, name FROM registration WHERE name = ?`, [sender], (err, row) => {
          if(err){
            ws.send(JSON.stringify({
              type: 'error',
              message: 'Ошибка БД'
            }));
            return;
          }
          const friendSocket2 = clientsId.get(row.id);
          friendSocket2.send(JSON.stringify({
            type: 'friend_accept',
            accept: true,
            message: `${get_invite_name}`,
            get_invite_name
          }));
        });

          db.run(`INSERT INTO friends (name, friend_name) VALUES (?, ?)`,
          [sender, namefriend]);

          db.run(`INSERT INTO friends (name, friend_name) VALUES (?, ?)`,
          [namefriend, sender]);
      }
      
      if (data.type === 'friends') {
        const name = String(data.name || '').trim();

        db.all(`SELECT friend_name FROM friends WHERE name = ?`, [name], (err, rows) => {
          if (err) {
            ws.send(JSON.stringify({
              type: 'error',
              message: 'Ошибка БД'
            }));
            return;
          }

          ws.send(JSON.stringify({
            type: 'friend_list',
            friends: rows
          }));
        });
      }
      if(data.type === 'friend_history'){
        const name = String(data.name || '').trim();
        
      }
    } catch (error) {
        ws.send(JSON.stringify({
          type: 'error',
          message: 'Некорректный JSON'
        }));
      }
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log('Пользователь отключился');
  });

  ws.on('error', (err) => {
    clients.delete(ws);
    console.error('Ошибка клиента:', err.message);
  });
});
