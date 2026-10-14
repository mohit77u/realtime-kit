import React, { useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { createRealtimeClient } from '@mohit77u/realtime-kit/client';
import { useChannel, useConnectionStatus, useDirect, useRoom } from '@mohit77u/realtime-kit/react';

const name = new URLSearchParams(location.search).get('name') || 'guest';
const rt = createRealtimeClient({
  url: `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/realtime`,
  auth: () => `demo-${name}`,
  autoConnect: false,
});

function App() {
  const status = useConnectionStatus(rt);
  const [msgs, setMsgs] = useState([]);
  const [text, setText] = useState('');
  const [dms, setDms] = useState([]);
  const [dmTo, setDmTo] = useState('');
  const [dmText, setDmText] = useState('');
  const [todos, setTodos] = useState([]);
  const [todoText, setTodoText] = useState('');
  const [notice, setNotice] = useState('');
  const [err, setErr] = useState('');

  // ROOM: join, live messages, live member list
  const room = useRoom(rt, 'public-general', (m) => setMsgs((x) => (x.some((y) => y.id === m.id) ? x : [...x, { id: m.id, ...m.data }])));
  useEffect(() => {
    if (room.joined) room.history({ limit: 20 }).then((h) => setMsgs((x) => [...h.map((d, i) => ({ id: `h${i}${d.at}`, ...d })), ...x]));
  }, [room.joined]);

  // DIRECT messages + per-user notifications
  useDirect(rt, (m) => (m.data?.notice ? setNotice(m.data.notice) : setDms((x) => [...x, { id: m.id, ...m.data }])));

  // TOPIC channel ("update in one place shows up everywhere")
  useChannel(rt, 'todos', (d) => setTodos((x) => [...x, d.todo]));
  useEffect(() => { if (status === 'open') rt.request('todo.list').then(setTodos); }, [status]);

  const run = (p) => p.then(() => setErr(''), (e) => setErr(`${e.code}: ${e.message}`));

  return (
    <div>
      <h3 id="me">{name}</h3>
      <p id="status">status: {status}</p>
      <p id="joined">joined: {String(room.joined)}</p>
      <p id="members">members: {room.members.join(',')}</p>
      <p id="err">{err}</p>
      <p id="notice">{notice}</p>
      <h4>Room</h4>
      <input id="text" value={text} onChange={(e) => setText(e.target.value)} />
      <button id="send" onClick={() => run(room.send({ text }).then(() => setText('')))}>send</button>
      <button id="try-private" onClick={() => run(rt.room('private-someone-else').join())}>join other's private room</button>
      <ul id="msgs">{msgs.map((m) => <li key={m.id}>{m.from}: {m.text}</li>)}</ul>
      <h4>Direct message</h4>
      <input id="dm-to" placeholder="user" value={dmTo} onChange={(e) => setDmTo(e.target.value)} />
      <input id="dm-text" value={dmText} onChange={(e) => setDmText(e.target.value)} />
      <button id="dm-send" onClick={() => run(rt.sendToUser(dmTo, { text: dmText }).then(() => setDmText('')))}>send dm</button>
      <ul id="dms">{dms.map((m) => <li key={m.id}>{m.from} → {m.to}: {m.text}</li>)}</ul>
      <h4>Todos (live)</h4>
      <input id="todo-text" value={todoText} onChange={(e) => setTodoText(e.target.value)} />
      <button id="todo-add" onClick={() => run(rt.request('todo.add', { title: todoText }).then(() => setTodoText('')))}>add</button>
      <ul id="todos">{todos.map((t) => <li key={t.id}>{t.title} (by {t.by})</li>)}</ul>
    </div>
  );
}
createRoot(document.getElementById('root')).render(<App />);
