# Express + React chat (realtime-kit example)

Shows, in one small app: **room chat with history and online members**, **direct messages**, **per-user notifications**, **live data updates** (todos shown in every open tab), **private-room denial**, and **two server instances sharing one Redis**.

```bash
npm install
npm run build                       # builds the React app into dist/
docker run -p 6379:6379 redis       # any Redis works (REDIS_URL=redis://... to change)

PORT=3000 npm start                 # terminal 1
PORT=3001 npm start                 # terminal 2 (second instance, same Redis)
```

Open two browsers:

- http://localhost:3000/?name=ann
- http://localhost:3001/?name=bob  (different server instance)

Chat in the room, send a direct message (`bob`), add a todo, and press "join other's private room" (denied). Everything crosses between the two instances through Redis.

Read `server.js` first: the whole backend is one file with comments for every realtime-kit option used.
