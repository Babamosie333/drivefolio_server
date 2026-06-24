import express from 'express'
import { WebSocketServer } from 'ws'
import mongoose from 'mongoose'
import { createServer } from 'http'
import { v4 as uuidv4 } from 'uuid'
import dotenv from 'dotenv'

dotenv.config()

// ═══════════════════════════════════
// DATABASE MODELS
// ═══════════════════════════════════

const whisperSchema = new mongoose.Schema({
    id:        { type: String, default: () => uuidv4() },
    message:   { type: String, required: true, maxlength: 30 },
    country:   { type: String, default: 'us' },
    x:         { type: Number, default: 0 },
    z:         { type: Number, default: 0 },
    createdAt: { type: Date, default: Date.now }
})
const Whisper = mongoose.model('Whisper', whisperSchema)

const scoreSchema = new mongoose.Schema({
    id:        { type: String, default: () => uuidv4() },
    time:      { type: Number, required: true },
    country:   { type: String, default: 'us' },
    createdAt: { type: Date, default: Date.now }
})
const Score = mongoose.model('Score', scoreSchema)

const cataclysmSchema = new mongoose.Schema({
    count:    { type: Number, default: 0 },
    progress: { type: Number, default: 0 }
})
const Cataclysm = mongoose.model('Cataclysm', cataclysmSchema)

const cookieSchema = new mongoose.Schema({
    count: { type: Number, default: 0 }
})
const Cookie = mongoose.model('Cookie', cookieSchema)

// ═══════════════════════════════════
// SERVER SETUP
// ═══════════════════════════════════

const app = express()
const server = createServer(app)
const wss = new WebSocketServer({ server })

app.use(express.json())

// CORS
app.use((req, res, next) =>
{
    res.header('Access-Control-Allow-Origin', '*')
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
    next()
})

// Health check
app.get('/', (req, res) =>
{
    res.json({
        status: 'Drivefolio server running!',
        clients: wss.clients.size,
        cache: {
            whispers: cache.whispers.length,
            scores: cache.scores.length,
            cataclysmCount: cache.cataclysmCount,
            cookiesCount: cache.cookiesCount
        }
    })
})

// ═══════════════════════════════════
// DATA CACHE
// ═══════════════════════════════════

let cache = {
    whispers:           [],
    scores:             [],
    cataclysmCount:     0,
    cataclysmProgress:  0,
    cookiesCount:       0,
}

const loadCache = async () =>
{
    try
    {
        cache.whispers = await Whisper
            .find()
            .sort({ createdAt: -1 })
            .limit(parseInt(process.env.MAX_WHISPERS) || 30)
            .lean()

        cache.scores = await Score
            .find()
            .sort({ time: 1 })
            .limit(20)
            .lean()

        let cataclysm = await Cataclysm.findOne().lean()
        if(!cataclysm)
        {
            const newCataclysm = await Cataclysm.create({ count: 0, progress: 0 })
            cataclysm = newCataclysm.toObject()
        }
        cache.cataclysmCount    = cataclysm.count
        cache.cataclysmProgress = cataclysm.progress

        let cookie = await Cookie.findOne().lean()
        if(!cookie)
        {
            const newCookie = await Cookie.create({ count: 0 })
            cookie = newCookie.toObject()
        }
        cache.cookiesCount = cookie.count

        console.log('✅ Cache loaded:', {
            whispers: cache.whispers.length,
            scores: cache.scores.length,
            cataclysmCount: cache.cataclysmCount,
            cookiesCount: cache.cookiesCount
        })
    }
    catch(error)
    {
        console.error('❌ Cache load error:', error)
    }
}

// ═══════════════════════════════════
// BROADCAST
// ═══════════════════════════════════

const broadcast = (data, excludeClient = null) =>
{
    const message = JSON.stringify(data)
    let count = 0
    wss.clients.forEach(client =>
    {
        if(client !== excludeClient && client.readyState === 1)
        {
            client.send(message)
            count++
        }
    })
    console.log(`📡 Broadcast [${data.type}] to ${count} clients`)
}

// ═══════════════════════════════════
// WEBSOCKET
// ═══════════════════════════════════

wss.on('connection', async (ws, req) =>
{
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress
    console.log(`✅ Client connected from ${ip}. Total: ${wss.clients.size}`)

    // Send init data
    const initData = {
        type:               'init',
        whispers:           cache.whispers,
        scores:             cache.scores,
        cataclysmCount:     cache.cataclysmCount,
        cataclysmProgress:  cache.cataclysmProgress,
        cookiesCount:       cache.cookiesCount,
    }
    ws.send(JSON.stringify(initData))
    console.log('📤 Sent init data to new client')

    ws.on('message', async (rawData) =>
    {
        try
        {
            const data = JSON.parse(rawData.toString())
            console.log(`📨 Received: ${data.type}`, data)

            // ── Whisper Insert ──
            if(data.type === 'whisperInsert')
            {
                if(!data.message)
                {
                    console.warn('⚠️ Whisper missing message')
                    return
                }

                const whisper = await Whisper.create({
                    id:      uuidv4(),
                    message: data.message.slice(0, 30),
                    country: data.country || 'us',
                    x:       data.x || 0,
                    z:       data.z || 0,
                })

                const whisperObj = whisper.toObject()
                cache.whispers.unshift(whisperObj)

                const max = parseInt(process.env.MAX_WHISPERS) || 30
                if(cache.whispers.length > max)
                {
                    const removed = cache.whispers.splice(max)
                    for(const w of removed)
                        await Whisper.deleteOne({ id: w.id })
                }

                broadcast({ type: 'whisperInsert', whisper: whisperObj })
                console.log(`✅ Whisper saved: "${data.message}"`)
            }

            // ── Score Insert ──
            else if(data.type === 'scoreInsert')
            {
                if(!data.time)
                {
                    console.warn('⚠️ Score missing time')
                    return
                }

                const score = await Score.create({
                    id:      uuidv4(),
                    time:    data.time,
                    country: data.country || 'us',
                })

                const scoreObj = score.toObject()
                cache.scores.push(scoreObj)
                cache.scores.sort((a, b) => a.time - b.time)
                cache.scores = cache.scores.slice(0, 20)

                broadcast({ type: 'scoresUpdate', scores: cache.scores })
                console.log(`✅ Score saved: ${data.time}s`)
            }

            // ── Cataclysm Insert ──
            else if(data.type === 'cataclysmInsert')
            {
                cache.cataclysmCount++
                cache.cataclysmProgress = Math.min(cache.cataclysmCount / 1000, 1)

                await Cataclysm.findOneAndUpdate(
                    {},
                    {
                        count:    cache.cataclysmCount,
                        progress: cache.cataclysmProgress
                    },
                    { upsert: true }
                )

                broadcast({
                    type:               'cataclysmUpdate',
                    cataclysmCount:     cache.cataclysmCount,
                    cataclysmProgress:  cache.cataclysmProgress,
                })
                console.log(`✅ Cataclysm count: ${cache.cataclysmCount}`)
            }

            // ── Cookies Insert ──
            else if(data.type === 'cookiesInsert')
            {
                cache.cookiesCount += data.amount || 1

                await Cookie.findOneAndUpdate(
                    {},
                    { count: cache.cookiesCount },
                    { upsert: true }
                )

                broadcast({
                    type:         'cookiesUpdate',
                    cookiesCount: cache.cookiesCount,
                })
                console.log(`✅ Cookies count: ${cache.cookiesCount}`)
            }

            else
            {
                console.warn(`⚠️ Unknown message type: ${data.type}`)
            }
        }
        catch(error)
        {
            console.error('❌ Message error:', error)
        }
    })

    ws.on('close', () =>
    {
        console.log(`❌ Client disconnected. Total: ${wss.clients.size}`)
    })

    ws.on('error', (error) =>
    {
        console.error('❌ WebSocket error:', error)
    })
})

// ═══════════════════════════════════
// KEEP ALIVE
// ═══════════════════════════════════

const SERVER_URL = process.env.RENDER_EXTERNAL_URL
if(SERVER_URL)
{
    setInterval(async () =>
    {
        try
        {
            await fetch(SERVER_URL)
            console.log('💓 Keep alive ping sent')
        }
        catch(error)
        {
            console.error('❌ Keep alive failed:', error)
        }
    }, 10 * 60 * 1000)
}

// ═══════════════════════════════════
// START
// ═══════════════════════════════════

const start = async () =>
{
    try
    {
        await mongoose.connect(process.env.MONGODB_URI)
        console.log('✅ MongoDB connected!')

        await loadCache()

        const PORT = process.env.PORT || 3000
        server.listen(PORT, () =>
        {
            console.log(`✅ Server running on port ${PORT}`)
        })
    }
    catch(error)
    {
        console.error('❌ Server start error:', error)
        process.exit(1)
    }
}

start()