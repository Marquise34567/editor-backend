import { createServer } from 'http'
import app from './app'
import { initRealtime } from './realtime'

const PORT = Number(process.env.PORT || 4000)

const server = createServer(app)
initRealtime(server)

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`)
})
